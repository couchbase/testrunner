def verify_rbac_exclusion_syntax(test_obj, rest, bucket_name, scope_name,
                                  allowed_col, excluded_col, role_suffix,
                                  runtype="default", service_validator=None,
                                  extra_roles=None):
    role_name = "excl_role_" + role_suffix
    username = "excl_user_" + role_suffix
    role_created = False
    user_created = False
    try:
        test_obj.log.info(
            "RBAC exclusion test: bucket=%s scope=%s allowed=%s excluded=%s"
            % (bucket_name, scope_name, allowed_col, excluded_col))
        if runtype == "default":
            rest.diag_eval(
                "persistent_term:put(config_profile, "
                "[{custom_roles_enabled, true} | "
                "persistent_term:get(config_profile, [])]).")
        permissions = {
            "cluster.collection[{b}:{s}:.]".format(b=bucket_name, s=scope_name): "all",
            "cluster.collection[{b}:{s}:{c}]".format(
                b=bucket_name, s=scope_name, c=excluded_col): "none",
        }
        status, resp = rest.create_custom_role(
            role_name, "Exclusion role - " + role_name, permissions)
        if not status:
            test_obj.log.warning(
                "Custom roles not supported on this cluster; skipping exclusion RBAC check: %s"
                % resp)
            return
        role_created = True
        roles_param = "{r},data_writer[{b}]".format(r=role_name, b=bucket_name)
        if extra_roles:
            roles_param += "," + extra_roles
        rest.add_set_builtin_user(
            username,
            "name={u}&password=password&roles={s}".format(
                u=username, s=roles_param))
        user_created = True
        perm_allowed = "cluster.collection[{b}:{s}:{c}]!write".format(
            b=bucket_name, s=scope_name, c=allowed_col)
        perm_excluded = "cluster.collection[{b}:{s}:{c}]!write".format(
            b=bucket_name, s=scope_name, c=excluded_col)
        result = rest.check_user_permission(
            username, "password", ",".join([perm_allowed, perm_excluded]))
        test_obj.assertTrue(result.get(perm_allowed, False),
            "RBAC exclusion: allowed collection '%s' should be accessible, got: %s"
            % (allowed_col, result))
        test_obj.assertFalse(result.get(perm_excluded, True),
            "RBAC exclusion: excluded collection '%s' should be denied, got: %s"
            % (excluded_col, result))
        test_obj.log.info(
            "RBAC exclusion permission check passed: allowed=%s(%s) excluded=%s(%s)"
            % (allowed_col, result.get(perm_allowed),
               excluded_col, result.get(perm_excluded)))
        if service_validator:
            service_validator(username, "password")
    finally:
        if user_created:
            try:
                rest.delete_builtin_user(username)
            except Exception:
                pass
        if role_created:
            rest.delete_custom_role(role_name)
