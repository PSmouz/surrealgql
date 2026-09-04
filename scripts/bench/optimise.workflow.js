export const meta = {
  name: 'bench-optimise',
  description: 'Profile a crud-bench port, hypothesise an engine optimisation from the flamegraph, apply it, and keep it only if the harness confirms a statistically significant speedup.',
  whenToUse: 'Run after porting/benching a crud-bench scan port when you want an AI-driven, measurement-gated optimisation pass on the SurrealDB engine for that workload.',
  phases: [
    { title: 'Baseline' },
    { title: 'Optimise' },
    { title: 'Report' },
  ],
}

// ---------------------------------------------------------------------------
// Invocation (workflows require explicit opt-in — this file is just the recipe):
//
//   Workflow({
//     scriptPath: 'scripts/bench/optimise.workflow.js',
//     args: { bench: 'scans/where_integer_eq_full', maxRounds: 4 }
//   })
//
// args:
//   bench      (required) substring filter selecting exactly ONE bench file.
//   maxRounds  (optional) number of hypothesise→apply→measure rounds. Default 3.
//   backend    (optional) storage backend. Default 'mem'.
//
// Pre-reqs: clean git working tree (the loop reverts rejected changes with
// `git restore`), and `samply` installed (profile.sh installs it on first use).
// ---------------------------------------------------------------------------

const bench = args?.bench
if (!bench) throw new Error('args.bench is required (a bench path filter)')
const backend = args?.backend ?? 'mem'
const maxRounds = args?.maxRounds ?? 3

const HYPOTHESIS = {
  type: 'object',
  required: ['kept', 'rationale', 'file', 'verdict'],
  properties: {
    kept: { type: 'boolean', description: 'true if the change was measured as a significant improvement and left in place' },
    rationale: { type: 'string', description: 'what hot path was targeted and why' },
    file: { type: 'string', description: 'primary source file edited (path), or "none" if no plausible change was found' },
    verdict: { type: 'string', description: 'the harness verdict line from measure.sh, verbatim' },
    change: { type: 'string', description: 'the harness `change : [...] (p = ..)` line, verbatim, or empty' },
  },
}

phase('Baseline')
// Establish and persist the baseline, and record a samply profile for reference.
const baseline = await agent(
  `You are establishing a benchmark baseline for the SurrealDB engine.
Run, from the repo root:
  scripts/bench/measure.sh '${bench}' --backend ${backend} --save
then:
  scripts/bench/profile.sh '${bench}' --backend ${backend}
Report the baseline time/median lines and the absolute path of the samply
profile (.json.gz) that profile.sh saved. Do NOT edit any source files.`,
  { phase: 'Baseline', label: 'baseline' },
)

phase('Optimise')
const rounds = []
for (let round = 0; round < maxRounds; round++) {
  const verdict = await agent(
    `Round ${round + 1}/${maxRounds} of profile-driven optimisation of the SurrealDB engine for bench '${bench}'.

Context from baseline step:
${baseline}

Do exactly this:
1. Identify a hot path for this workload. Trace the engine code that executes the
   bench's query under surrealdb/core/ (the SELECT/CREATE/UPDATE/etc. execution
   path, index lookups, value decoding, etc.). The samply profile from the
   baseline step is available for a human to inspect; rely on code reading plus
   the measure.sh delta below as the ground truth.
2. Find ONE focused, behaviour-preserving optimisation (avoid a clone, hoist work
   out of a hot loop, cheaper data structure, skip redundant recompute, etc.).
   Do NOT change query semantics or test results.
3. Apply the change with Edit.
4. Re-measure: run  scripts/bench/measure.sh '${bench}' --backend ${backend}
   (NO --save). Read the verdict line.
5. Decide:
   - If the verdict is "Performance has improved" with p < 0.05 → KEEP the change.
   - Otherwise → revert it: \`git restore <files you edited>\` (and \`git clean -fd\`
     any new files), leaving the tree exactly as you found it.
6. If you find no plausible safe change, set file:"none", kept:false and do not
   edit anything.

Return the structured verdict. Be honest: only set kept:true if the harness
actually reported a significant improvement.`,
    { phase: 'Optimise', label: `round-${round + 1}`, schema: HYPOTHESIS },
  )
  rounds.push({ round: round + 1, ...verdict })
  log(`round ${round + 1}: kept=${verdict.kept} file=${verdict.file} :: ${verdict.verdict}`)

  // If a round produced a kept improvement, re-profile so the next round works
  // from the new hot path rather than the stale baseline profile.
  if (verdict.kept) {
    await agent(
      `An optimisation was just kept for bench '${bench}'. Re-profile to refresh
the samply profile for the next round: run  scripts/bench/profile.sh '${bench}'
--backend ${backend}. Report the saved profile path. Do NOT edit source.`,
      { phase: 'Optimise', label: `reprofile-${round + 1}` },
    )
  }
}

phase('Report')
const kept = rounds.filter((r) => r.kept)
const report = await agent(
  `Write a concise markdown report named BENCH_REPORT.md at the repo root summarising
this optimisation run for bench '${bench}'.

Rounds (JSON):
${JSON.stringify(rounds, null, 2)}

For each KEPT change include: the file, the rationale, and the harness change line
(speedup + p-value). List rejected hypotheses briefly so they aren't re-tried.
End with the net result. Write the file and return its path.`,
  { phase: 'Report', label: 'report' },
)

return {
  bench,
  backend,
  rounds,
  keptCount: kept.length,
  report,
}
