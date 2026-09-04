//! This module was taken from the criterion and then modified for use in the language test suite.
//!
//!
//! [Criterion]'s statistics library.
//!
//! [Criterion]: https://github.com/bheisler/criterion.rs

//pub mod bivariate;
pub mod tuple;
pub mod univariate;

mod rand_util;

use std::mem;
use std::ops::Deref;

use surrealdb_types::SurrealValue;

use crate::cmd::bench::stats::univariate::outliers::tukey::{self, Label};
use crate::cmd::bench::stats::univariate::{Sample, mixed};
use crate::cmd::bench::{DEFAULT_CONFIDENCE_LEVEL, DEFAULT_RESAMPLES};

/// The bootstrap distribution of some parameter
#[derive(Clone)]
pub struct Distribution(Box<[f64]>);

impl Distribution {
	/// Create a distribution from the given values
	pub fn from(values: Box<[f64]>) -> Distribution {
		Distribution(values)
	}

	/// Computes the confidence interval of the population parameter using percentiles
	///
	/// # Panics
	///
	/// Panics if the `confidence_level` is not in the `(0, 1)` range.
	pub fn confidence_interval(&self, confidence_level: f64) -> (f64, f64) {
		assert!(confidence_level > 0.0 && confidence_level < 1.0);

		let percentiles = self.percentiles();

		// FIXME(privacy) this should use the `at_unchecked()` method
		(
			percentiles.at(50.0 * (1.0 - confidence_level)),
			percentiles.at(50.0 * (1.0 + confidence_level)),
		)
	}

	/// Computes the "likelihood" of seeing the value `t` or "more extreme" values in the
	/// distribution.
	pub fn p_value(&self, t: f64, tails: &Tails) -> f64 {
		use std::cmp;

		let n = self.0.len();
		let hits = self.0.iter().filter(|&&x| x < t).count();

		let tails = match *tails {
			Tails::One => 1.0,
			Tails::Two => 2.0,
		};

		cmp::min(hits, n - hits) as f64 / n as f64 * tails
	}
}

impl Deref for Distribution {
	type Target = Sample;

	fn deref(&self) -> &Sample {
		let slice: &[_] = &self.0;

		unsafe { mem::transmute(slice) }
	}
}

/// Number of tails for significance testing
pub enum Tails {
	/// One tailed test
	One,
	/// Two tailed test
	Two,
}

/// An estimate of a measured value
#[derive(Clone, SurrealValue)]
pub struct Estimate {
	/// The confidence level with which the upper_bound and lower_bound where created.
	pub confidence_level: f64,
	pub lower_bound: f64,
	pub upper_bound: f64,
	/// The measured value
	pub point: f64,
	pub standard_error: f64,
}

impl Estimate {
	pub fn from_point_distribution(point: f64, distr: &Distribution, cl: f64) -> Self {
		let (lower_bound, upper_bound) = distr.confidence_interval(cl);
		Self {
			lower_bound,
			upper_bound,
			confidence_level: cl,
			point,
			standard_error: distr.std_dev(None),
		}
	}
}

#[derive(Clone, SurrealValue)]
pub struct MeasurementData {
	pub iterations: Vec<f64>,
	pub times: Vec<f64>,
	pub average_times: Vec<f64>,
	pub labels: Vec<Label>,
	pub fences: (f64, f64, f64, f64),
	pub mean: Estimate,
	pub median: Estimate,
	pub abs_dev: Estimate,
	pub std_dev: Estimate,
}

impl MeasurementData {
	/// Builds the measurement statistics from the collected sample times, or
	/// `None` when there are too few samples to compute them. The underlying
	/// `Sample` routines require at least two finite data points (`Sample::new`
	/// asserts `len > 1` and no `NaN`), so a bench whose single iteration exceeds
	/// `max_time` — collecting only one sample — would otherwise panic here.
	/// Callers must handle `None` (skip the bench) rather than unwrapping.
	pub fn from_iteration_times(iterations: Vec<f64>, times: Vec<f64>) -> Option<Self> {
		let avg_time_vec = iterations
			.iter()
			.zip(times.iter())
			.map(|(&iters, &samples)| samples / iters)
			.collect::<Vec<f64>>();
		if avg_time_vec.len() < 2 || avg_time_vec.iter().any(|x| x.is_nan()) {
			return None;
		}
		let avg_time = Sample::new(&avg_time_vec);

		let labeled_sample = tukey::classify(avg_time);
		let labels = labeled_sample.iter().map(|(_, x)| x).collect();

		fn stats(sample: &Sample) -> (f64, f64, f64, f64) {
			let mean = sample.mean();
			let std_dev = sample.std_dev(Some(mean));
			let median = sample.percentiles().median();
			let mad = sample.median_abs_dev(Some(median));
			(mean, std_dev, median, mad)
		}

		let point_est = stats(avg_time);
		let bootstrap = avg_time.bootstrap(DEFAULT_RESAMPLES, stats);

		let mean_est =
			Estimate::from_point_distribution(point_est.0, &bootstrap.0, DEFAULT_CONFIDENCE_LEVEL);
		let std_dev_est =
			Estimate::from_point_distribution(point_est.1, &bootstrap.1, DEFAULT_CONFIDENCE_LEVEL);
		let median_est =
			Estimate::from_point_distribution(point_est.2, &bootstrap.2, DEFAULT_CONFIDENCE_LEVEL);
		let mad_est =
			Estimate::from_point_distribution(point_est.3, &bootstrap.3, DEFAULT_CONFIDENCE_LEVEL);

		Some(MeasurementData {
			iterations,
			times,
			fences: labeled_sample.fences(),
			labels,
			average_times: avg_time_vec,
			mean: mean_est,
			median: median_est,
			abs_dev: mad_est,
			std_dev: std_dev_est,
		})
	}
}

pub struct ComparisonData {
	pub dist_mean: Estimate,
	//pub dist_median: Estimate,
	//pub t: Estimate,
	pub p_value: f64,
	/// Median per-iteration time (seconds) of the baseline being compared against,
	/// so a report can show the baseline's absolute time next to the current run.
	pub base_median: f64,
}

impl ComparisonData {
	pub fn compare(base: &MeasurementData, current: &MeasurementData) -> Self {
		fn stats(a: &Sample, b: &Sample) -> (f64, f64) {
			(a.mean() / b.mean() - 1., a.percentiles().median() / b.percentiles().median() - 1.)
		}

		let avg_times = Sample::new(&current.average_times);
		let base_avg_times = Sample::new(&base.average_times);

		let (dist_mean, _dist_median) =
			univariate::bootstrap(avg_times, base_avg_times, DEFAULT_RESAMPLES, stats);

		let (mean, _median) = stats(avg_times, base_avg_times);

		let dist_mean =
			Estimate::from_point_distribution(mean, &dist_mean, DEFAULT_CONFIDENCE_LEVEL);
		//let dist_median = Estimate::from_point_distribution(median, &dist_median,
		// DEFAULT_CONFIDENCE_LEVEL);

		let t_stat = avg_times.t(base_avg_times);
		let (t_dist,) =
			mixed::bootstrap(avg_times, base_avg_times, DEFAULT_RESAMPLES, |a, b| (a.t(b),));

		//let t = Estimate::from_point_distribution(t_stat, &t_dist, DEFAULT_CONFIDENCE_LEVEL);

		let p_value = t_dist.p_value(t_stat, &Tails::Two);

		ComparisonData {
			dist_mean,
			//dist_median,
			//t,
			p_value,
			base_median: base.median.point,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::MeasurementData;

	// Regression: a bench whose single iteration exceeds `max_time` collects only
	// one sample. `Sample::new` asserts `len > 1`, so building statistics from it
	// used to panic ("Measurement thread paniced") and fail the whole run. It must
	// now return `None` so the caller can skip the bench instead.
	#[test]
	fn from_iteration_times_needs_at_least_two_samples() {
		assert!(MeasurementData::from_iteration_times(vec![1.0], vec![0.5]).is_none());
		assert!(MeasurementData::from_iteration_times(vec![], vec![]).is_none());
	}

	// A non-finite average (here `0.0 / 0.0`) would also trip `Sample::new`'s
	// no-NaN assertion; reject it rather than panic.
	#[test]
	fn from_iteration_times_rejects_nan() {
		assert!(MeasurementData::from_iteration_times(vec![1.0, 0.0], vec![1.0, 0.0]).is_none());
	}

	// Two or more finite samples still produce statistics as before.
	#[test]
	fn from_iteration_times_builds_with_two_samples() {
		let m = MeasurementData::from_iteration_times(vec![1.0, 1.0], vec![0.5, 0.7])
			.expect("two samples should produce statistics");
		assert_eq!(m.average_times, vec![0.5, 0.7]);
	}
}
