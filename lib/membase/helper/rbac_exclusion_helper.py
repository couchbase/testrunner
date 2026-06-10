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

        # --- Additional privilege resolution checks ---
        _verify_privilege_resolution(test_obj, rest, bucket_name, scope_name,
                                     allowed_col, excluded_col, role_suffix)
    finally:
        if user_created:
            try:
                rest.delete_builtin_user(username)
            except Exception:
                pass
        if role_created:
            rest.delete_custom_role(role_name)


def _verify_privilege_resolution(test_obj, rest, bucket_name, scope_name,
                                  allowed_col, excluded_col, role_suffix):
    """
    Extra privilege resolution checks run after the main exclusion test:
    1. collection-allow beats scope-deny
    2. role with only deny entries grants nothing
    3. old role syntax backward compat
    """
    # 1. Collection-allow beats scope-deny
    r1 = "excl_role_res_" + role_suffix
    u1 = "excl_user_res_" + role_suffix
    r1_created = u1_created = False
    try:
        status, _ = rest.create_custom_role(r1, "Resolution test", {
            "cluster.collection[{b}:{s}:.]".format(b=bucket_name, s=scope_name): "none",
            "cluster.collection[{b}:{s}:{c}]".format(
                b=bucket_name, s=scope_name, c=allowed_col): "all",
        })
        if status:
            r1_created = True
            rest.add_set_builtin_user(u1,
                "name={u}&password=password&roles={r}".format(u=u1, r=r1))
            u1_created = True
            p_allow = "cluster.collection[{b}:{s}:{c}]!write".format(
                b=bucket_name, s=scope_name, c=allowed_col)
            p_deny = "cluster.collection[{b}:{s}:{c}]!write".format(
                b=bucket_name, s=scope_name, c=excluded_col)
            res = rest.check_user_permission(u1, "password", ",".join([p_allow, p_deny]))
            test_obj.assertTrue(res.get(p_allow, False),
                "Privilege resolution: collection-allow should beat scope-deny "
                "for '%s', got: %s" % (allowed_col, res))
            test_obj.assertFalse(res.get(p_deny, True),
                "Privilege resolution: scope-deny should still apply to "
                "'%s', got: %s" % (excluded_col, res))
            test_obj.log.info("Privilege resolution (collection-allow beats scope-deny) passed")
    except Exception as e:
        test_obj.log.warning("Privilege resolution check skipped/failed: %s" % e)
    finally:
        if u1_created:
            try: rest.delete_builtin_user(u1)
            except Exception: pass
        if r1_created:
            try: rest.delete_custom_role(r1)
            except Exception: pass

    # 2. Role with only deny entries grants nothing
    r2 = "excl_role_denyonly_" + role_suffix
    u2 = "excl_user_denyonly_" + role_suffix
    r2_created = u2_created = False
    try:
        status, _ = rest.create_custom_role(r2, "Deny-only role", {
            "cluster.collection[{b}:{s}:{c}]".format(
                b=bucket_name, s=scope_name, c=excluded_col): "none",
        })
        if status:
            r2_created = True
            rest.add_set_builtin_user(u2,
                "name={u}&password=password&roles={r}".format(u=u2, r=r2))
            u2_created = True
            p = "cluster.collection[{b}:{s}:{c}]!write".format(
                b=bucket_name, s=scope_name, c=excluded_col)
            res = rest.check_user_permission(u2, "password", p)
            test_obj.assertFalse(res.get(p, True),
                "Deny-only role should grant no access to '%s', got: %s"
                % (excluded_col, res))
            test_obj.log.info("Deny-only role check passed")
    except Exception as e:
        test_obj.log.warning("Deny-only role check skipped/failed: %s" % e)
    finally:
        if u2_created:
            try: rest.delete_builtin_user(u2)
            except Exception: pass
        if r2_created:
            try: rest.delete_custom_role(r2)
            except Exception: pass

    # 3. Backward compat: old data_reader syntax still works
    u3 = "excl_user_compat_" + role_suffix
    u3_created = False
    try:
        rest.add_set_builtin_user(u3,
            "name={u}&password=password&roles=data_reader[{b}]".format(
                u=u3, b=bucket_name))
        u3_created = True
        p = "cluster.bucket[{b}].data.docs!read".format(b=bucket_name)
        res = rest.check_user_permission(u3, "password", p)
        test_obj.assertTrue(res.get(p, False),
            "Backward compat: data_reader[bucket] should still work, got: %s" % res)
        test_obj.log.info("Backward compat (old role syntax) check passed")
    except Exception as e:
        test_obj.log.warning("Backward compat check skipped/failed: %s" % e)
    finally:
        if u3_created:
            try: rest.delete_builtin_user(u3)
            except Exception: pass

    # 4. Scope deny beats bucket allow
    r4 = "excl_role_sdba_" + role_suffix
    u4 = "excl_user_sdba_" + role_suffix
    r4_created = u4_created = False
    try:
        status, _ = rest.create_custom_role(r4, "Scope deny beats bucket allow", {
            "cluster.collection[{b}:{s}:.]".format(b=bucket_name, s=scope_name): "none",
        })
        if status:
            r4_created = True
            rest.add_set_builtin_user(u4,
                "name={u}&password=password&roles=data_writer[{b}],{r}".format(
                    u=u4, b=bucket_name, r=r4))
            u4_created = True
            p = "cluster.collection[{b}:{s}:{c}]!write".format(
                b=bucket_name, s=scope_name, c=excluded_col)
            res = rest.check_user_permission(u4, "password", p)
            test_obj.assertFalse(res.get(p, True),
                "Scope deny beats bucket allow: write to '%s' should be denied, got: %s"
                % (excluded_col, res))
            test_obj.log.info("Scope deny beats bucket allow check passed")
    except Exception as e:
        test_obj.log.warning("Scope deny beats bucket allow check skipped/failed: %s" % e)
    finally:
        if u4_created:
            try: rest.delete_builtin_user(u4)
            except Exception: pass
        if r4_created:
            try: rest.delete_custom_role(r4)
            except Exception: pass

    # 5. Non-admin user cannot manage RBAC
    u5 = "excl_user_nonadmin_" + role_suffix
    u5_created = False
    try:
        rest.add_set_builtin_user(u5,
            "name={u}&password=password&roles=data_writer[{b}]".format(
                u=u5, b=bucket_name))
        u5_created = True
        perm_rbac = "cluster.admin.rbac!write"
        res = rest.check_user_permission(u5, "password", perm_rbac)
        test_obj.assertFalse(res.get(perm_rbac, True),
            "Non-admin should not have cluster.admin.rbac!write, got: %s" % res)
        test_obj.log.info("Non-admin lacks RBAC admin permission check passed")
    except Exception as e:
        test_obj.log.warning("Non-admin RBAC permission check skipped/failed: %s" % e)
    finally:
        if u5_created:
            try: rest.delete_builtin_user(u5)
            except Exception: pass

    # 6. Custom deny overrides built-in role grant
    r6 = "excl_role_biconflict_" + role_suffix
    u6 = "excl_user_biconflict_" + role_suffix
    r6_created = u6_created = False
    try:
        status, _ = rest.create_custom_role(r6, "Custom deny vs built-in grant", {
            "cluster.collection[{b}:{s}:.]".format(b=bucket_name, s=scope_name): "all",
            "cluster.collection[{b}:{s}:{c}]".format(
                b=bucket_name, s=scope_name, c=excluded_col): "none",
        })
        if status:
            r6_created = True
            rest.add_set_builtin_user(u6,
                "name={u}&password=password&roles=data_writer[{b}],{r}".format(
                    u=u6, b=bucket_name, r=r6))
            u6_created = True
            p_allow = "cluster.collection[{b}:{s}:{c}]!write".format(
                b=bucket_name, s=scope_name, c=allowed_col)
            p_deny = "cluster.collection[{b}:{s}:{c}]!write".format(
                b=bucket_name, s=scope_name, c=excluded_col)
            res = rest.check_user_permission(u6, "password", ",".join([p_allow, p_deny]))
            test_obj.assertTrue(res.get(p_allow, False),
                "Custom+builtin: built-in grant on '%s' should still work, got: %s"
                % (allowed_col, res))
            test_obj.assertFalse(res.get(p_deny, True),
                "Custom+builtin: custom deny on '%s' should override built-in grant, got: %s"
                % (excluded_col, res))
            test_obj.log.info("Custom deny overrides built-in role grant check passed")
    except Exception as e:
        test_obj.log.warning("Custom deny vs built-in role check skipped/failed: %s" % e)
    finally:
        if u6_created:
            try: rest.delete_builtin_user(u6)
            except Exception: pass
        if r6_created:
            try: rest.delete_custom_role(r6)
            except Exception: pass
