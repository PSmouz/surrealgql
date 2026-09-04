/**
[env]
imports = ["reproductions/order_by_field_select_perm_leak_import.surql"]

[test]
reason = "Import: defines functions (as root) whose body sorts `secret` by the restricted `code` field. Wrapping the SELECT in a DEFINE FUNCTION body forces the runtime-resolved DynamicScan access path (the same construct as reproduction 7130), so the caller exercises the execute-time ORDER BY guard in operators/scan/dynamic.rs rather than the plan-time guard."
run = false
*/

DEFINE FUNCTION OVERWRITE fn::secret_ids_by_code() {
    RETURN SELECT VALUE id FROM secret ORDER BY code;
};

DEFINE FUNCTION OVERWRITE fn::secret_ids_by_code_desc() {
    RETURN SELECT VALUE id FROM secret ORDER BY code DESC;
};

DEFINE FUNCTION OVERWRITE fn::secret_ids_by_code_limited() {
    RETURN SELECT VALUE id FROM secret ORDER BY code LIMIT 2;
};
