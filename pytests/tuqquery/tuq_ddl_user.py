from tuqquery.tuq import QueryTests
from membase.api.exception import CBQError

class QueryDDLUserTests(QueryTests):
    def setUp(self):
        super(QueryDDLUserTests, self).setUp()
        self.bucket = "default"
        # Cleanup: drop all users and groups before each test, except cbadminbucket
        # Drop all users except cbadminbucket
        users_res = self.run_cbq_query("SELECT id, domain FROM system:user_info")
        for user in users_res['results']:
            user_id = user['id']
            domain = user['domain']
            if user_id and domain == "local" and user_id != "cbadminbucket":
                try:
                    self.run_cbq_query(f"DROP USER {user_id}")
                except Exception:
                    pass
        # Drop all groups
        groups_res = self.run_cbq_query("SELECT id FROM system:group_info")
        for group in groups_res['results']:
            group_name = group['id']
            if group_name:
                try:
                    self.run_cbq_query(f"DROP GROUP {group_name}")
                except Exception:
                    pass

    def suite_setUp(self):
        super(QueryDDLUserTests, self).suite_setUp()

    def tearDown(self):
        super(QueryDDLUserTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryDDLUserTests, self).suite_tearDown()

    def _get_user_info(self, user):
        res = self.run_cbq_query(f"SELECT * FROM system:user_info WHERE id = '{user}'")
        if res['results']:
            return res['results'][0]['user_info']
        return None

    def test_create_user_basic(self):
        user = "user1"
        password = "pass123"  # Password must be at least 6 characters
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        info = self._get_user_info(user)
        self.assertIsNotNone(info, "User info should exist after creation")
        self.assertEqual(info['id'], user)
        self.assertEqual(info['domain'], "local")
        self.assertEqual(info['groups'], [])
        self.assertEqual(info['roles'], [])
        self.assertNotIn("name", info)
        self.run_cbq_query(f"DROP USER {user}")

    def test_create_user_with_name(self):
        user = "user2"
        password = "pass234"
        name = "Test User"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}" WITH "{name}"')
        info = self._get_user_info(user)
        self.assertIsNotNone(info, "User info should exist after creation")
        self.assertEqual(info['id'], user)
        self.assertEqual(info['domain'], "local")
        self.assertIn("name", info)
        self.assertEqual(info["name"], name)
        self.run_cbq_query(f"DROP USER {user}")

    def test_create_user_duplicate(self):
        user = "user3"
        password = "pass345"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        self.run_cbq_query(f"DROP USER {user}")

    def test_drop_user(self):
        user = "user5"
        password = "pass567"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        self.run_cbq_query(f"DROP USER {user}")
        info = self._get_user_info(user)
        self.assertIsNone(info, "User info should not exist after drop")
        # Dropping again should fail (no IF EXISTS support)
        with self.assertRaises(CBQError):
            self.run_cbq_query(f"DROP USER {user}")

    def test_create_group_and_user_with_group(self):
        group = "group1"
        group_desc = "Test Group"
        user = "user6"
        password = "pass678"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        # Create group
        self.run_cbq_query(f'CREATE GROUP {group} WITH "{group_desc}" NO ROLES')
        # Create user in group
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}" GROUP {group}')
        info = self._get_user_info(user)
        self.assertIsNotNone(info, "User info should exist after creation")
        self.assertIn(group, info['groups'])
        self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f"DROP GROUP {group}")

    def test_create_user_with_no_groups(self):
        user = "user7"
        password = "pass789"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}" NO GROUPS')
        info = self._get_user_info(user)
        self.assertIsNotNone(info, "User info should exist after creation")
        self.assertEqual(info['groups'], [])
        self.run_cbq_query(f"DROP USER {user}")

    def test_create_user_with_multiple_groups(self):
        group1 = "group2"
        group2 = "group3"
        user = "user8"
        password = "pass890"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        self.run_cbq_query(f'CREATE GROUP {group1} NO ROLES')
        self.run_cbq_query(f'CREATE GROUP {group2} NO ROLES')
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}" GROUPS {group1}, {group2}')
        info = self._get_user_info(user)
        self.assertIsNotNone(info, "User info should exist after creation")
        self.assertIn(group1, info['groups'])
        self.assertIn(group2, info['groups'])
        self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f"DROP GROUP {group1}")
        self.run_cbq_query(f"DROP GROUP {group2}")

    def test_drop_user_no_if_exists(self):
        user = "user9"
        password = "pass901"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        # Drop non-existent user should fail (no IF EXISTS support)
        with self.assertRaises(CBQError):
            self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        self.run_cbq_query(f"DROP USER {user}")
        with self.assertRaises(CBQError):
            self.run_cbq_query(f"DROP USER {user}")

    def test_create_user_with_query_select_role(self):
        user = "user10"
        password = "pass012"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        # Create user with query_select role on default bucket
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        self.run_cbq_query(f'GRANT query_select, query_use_sequential_scans ON `{self.bucket}` TO {user}')
        info = self._get_user_info(user)
        self.assertIsNotNone(info, "User info should exist after creation")
        # Should have at least one role
        # self.assertTrue(any(r.get("role") == "query_select" and r.get("bucket_name") == self.bucket for r in info.get("roles", [])))
        # Try to run a SELECT as this user
        result = self.run_cbq_query(f"SELECT * FROM `{self.bucket}` LIMIT 1", username=user, password=password)
        self.assertIn("results", result)
        self.run_cbq_query(f"DROP USER {user}")

    def test_create_group_with_role_and_user_in_group(self):
        group = "group4"
        user = "user11"
        password = "pass112"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        # Create group with query_select role
        self.run_cbq_query(f'CREATE GROUP {group} ROLE query_select ON `{self.bucket}`')
        # Verify group has the correct role
        group_info_res = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group}'")
        self.assertTrue(group_info_res['results'], f"Group {group} should exist in system:group_info")
        group_info = group_info_res['results'][0]['group_info']
        roles = group_info.get("roles", [])
        # There should be at least one role, and it should match the expected structure
        found = False
        for r in roles:
            if (
                r.get("role") == "select"
                and r.get("bucket_name") == self.bucket
                and r.get("scope_name") == "*"
                and r.get("collection_name") == "*"
            ):
                found = True
                break
        self.assertTrue(found, f"Group {group} should have role 'select' on bucket '{self.bucket}' with scope '*' and collection '*'")
        # Create user in group
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}" GROUP {group}')
        info = self._get_user_info(user)
        self.assertIsNotNone(info, "User info should exist after creation")
        self.assertIn(group, info['groups'])
        # Should inherit role from group
        # self.assertTrue(any(r.get("role") == "query_select" and r.get("bucket_name") == self.bucket for r in info.get("roles", [])))
        # Try to run a SELECT as this user
        # Grant query_use_sequential_scans to user as well
        self.run_cbq_query(f'GRANT query_use_sequential_scans ON `{self.bucket}` TO {user}')
        result = self.run_cbq_query(f"SELECT * FROM `{self.bucket}` LIMIT 1", username=user, password=password)
        self.assertIn("results", result)
        self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f"DROP GROUP {group}")

    def test_create_group_with_multiple_privileges_and_user_in_group(self):
        """
        Test creating a group with multiple privileges (select, insert) and a user in that group.
        """
        group = "group_multi_priv"
        user = "user_multi_priv"
        password = "passmulti"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        # Create group with query_select and query_insert roles
        self.run_cbq_query(
            f'CREATE GROUP {group} ROLES query_select ON `{self.bucket}`, query_insert ON `{self.bucket}`, query_use_sequential_scans ON `{self.bucket}`'
        )
        # Verify group has both roles
        group_info_res = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group}'")
        self.assertTrue(group_info_res['results'], f"Group {group} should exist in system:group_info")
        group_info = group_info_res['results'][0]['group_info']
        roles = group_info.get("roles", [])
        found_select = False
        found_insert = False
        found_seqscan = False
        for r in roles:
            if (
                r.get("role") == "select"
                and r.get("bucket_name") == self.bucket
                and r.get("scope_name") == "*"
                and r.get("collection_name") == "*"
            ):
                found_select = True
            if (
                r.get("role") == "insert"
                and r.get("bucket_name") == self.bucket
                and r.get("scope_name") == "*"
                and r.get("collection_name") == "*"
            ):
                found_insert = True
            if (
                r.get("role") == "query_use_sequential_scans"
                and r.get("bucket_name") == self.bucket
                and r.get("scope_name") == "*"
                and r.get("collection_name") == "*"
            ):
                found_seqscan = True
        self.assertTrue(found_select, f"Group {group} should have role 'select' on bucket '{self.bucket}'")
        self.assertTrue(found_insert, f"Group {group} should have role 'insert' on bucket '{self.bucket}'")
        self.assertTrue(found_seqscan, f"Group {group} should have role 'query_use_sequential_scans' on bucket '{self.bucket}'")
        # Create user in group
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}" GROUP {group}')
        info = self._get_user_info(user)
        self.assertIsNotNone(info, "User info should exist after creation")
        self.assertIn(group, info['groups'])
        # Should inherit both roles from group
        # Try to run a SELECT as this user
        result = self.run_cbq_query(f"SELECT * FROM `{self.bucket}` LIMIT 1", username=user, password=password)
        self.assertIn("results", result)
        # Try to run an INSERT as this user (should succeed)
        try:
            self.run_cbq_query(
                f"INSERT INTO `{self.bucket}` (KEY, VALUE) VALUES ('test_multi_priv_key', {{'val': 1}})",
                username=user,
                password=password
            )
        finally:
            # Clean up inserted doc if possible
            try:
                self.run_cbq_query(
                    f"DELETE FROM `{self.bucket}` WHERE META().id = 'test_multi_priv_key'"
                )
            except Exception:
                pass
        self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f"DROP GROUP {group}")

    def test_create_user_with_short_password(self):
        user = "user_short"
        short_password = "123"  # Less than 6 chars
        # Try to create user with short password, should fail
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'CREATE USER {user} PASSWORD "{short_password}"')
        # Also check that user was not created
        info = self._get_user_info(user)
        self.assertIsNone(info, "User with short password should not be created")

    def test_create_user_with_special_char_password(self):
        user = "user_special"
        password = "p@$$w0rd!"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")
        # Should allow special characters in password
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        info = self._get_user_info(user)
        self.assertIsNotNone(info, "User info should exist after creation")
        self.run_cbq_query(f"DROP USER {user}")

    def test_create_user_and_group_rbac_admin(self):
        """
        Test that a user with admin privilege can create users and groups.
        """
        admin_user = "admin_user"
        admin_password = "adminpass123"
        test_user = "rbac_user"
        test_user_password = "rbacpass123"
        test_group = "rbac_group"

        # Create admin user and grant admin role
        self.run_cbq_query(f'CREATE USER {admin_user} PASSWORD "{admin_password}"')
        self.run_cbq_query(f"GRANT admin TO {admin_user}")

        try:
            # Create group as admin
            self.run_cbq_query(
                f'CREATE GROUP {test_group} WITH "RBAC Test Group" NO ROLES',
                username=admin_user,
                password=admin_password
            )
            # Create user as admin
            self.run_cbq_query(
                f'CREATE USER {test_user} PASSWORD "{test_user_password}" GROUP {test_group}',
                username=admin_user,
                password=admin_password
            )
            # Verify user and group were created
            user_info = self._get_user_info(test_user)
            self.assertIsNotNone(user_info, "User should be created by admin user")
            self.assertIn(test_group, user_info.get("groups", []))
        finally:
            # Cleanup
            self.run_cbq_query(f"DROP USER {test_user}", username=admin_user, password=admin_password)
            self.run_cbq_query(f"DROP GROUP {test_group}", username=admin_user, password=admin_password)
            self.run_cbq_query(f"DROP USER {admin_user}")

    def test_create_user_and_group_rbac_secadmin(self):
        """
        Test that a user with secadmin privilege can create users and groups.
        """
        secadmin_user = "secadmin_user"
        secadmin_password = "secadminpass123"
        test_user = "rbac_user_sec"
        test_user_password = "rbacpasssec123"
        test_group = "rbac_group_sec"

        # Create secadmin user and grant secadmin role
        self.run_cbq_query(f'CREATE USER {secadmin_user} PASSWORD "{secadmin_password}"')
        self.run_cbq_query(f"GRANT security_admin TO {secadmin_user}")

        try:
            # Create group as secadmin
            self.run_cbq_query(
                f'CREATE GROUP {test_group} WITH "RBAC Secadmin Test Group" NO ROLES',
                username=secadmin_user,
                password=secadmin_password
            )
            # Create user as secadmin
            self.run_cbq_query(
                f'CREATE USER {test_user} PASSWORD "{test_user_password}" GROUP {test_group}',
                username=secadmin_user,
                password=secadmin_password
            )
            # Verify user and group were created
            user_info = self._get_user_info(test_user)
            self.assertIsNotNone(user_info, "User should be created by secadmin user")
            self.assertIn(test_group, user_info.get("groups", []))
        finally:
            # Cleanup
            self.run_cbq_query(f"DROP USER {test_user}", username=secadmin_user, password=secadmin_password)
            self.run_cbq_query(f"DROP GROUP {test_group}", username=secadmin_user, password=secadmin_password)
            self.run_cbq_query(f"DROP USER {secadmin_user}")

    def test_create_user_and_group_rbac_ro_admin(self):
        """
        Test that a user with ro_admin privilege cannot create users or groups.
        """
        ro_user = "ro_admin_user"
        ro_password = "roadminpass123"
        test_user = "rbac_user2"
        test_user_password = "rbacpass234"
        test_group = "rbac_group2"

        # Create ro_admin user and grant ro_admin role
        self.run_cbq_query(f'CREATE USER {ro_user} PASSWORD "{ro_password}"')
        self.run_cbq_query(f"GRANT ro_admin TO {ro_user}")

        try:
            # Try to create group as ro_admin, should fail
            with self.assertRaises(CBQError):
                self.run_cbq_query(
                    f'CREATE GROUP {test_group} WITH "RBAC Test Group 2" NO ROLES',
                    username=ro_user,
                    password=ro_password
                )
            # Try to create user as ro_admin, should fail
            with self.assertRaises(CBQError):
                self.run_cbq_query(
                    f'CREATE USER {test_user} PASSWORD "{test_user_password}" NO ROLES',
                    username=ro_user,
                    password=ro_password
                )
            # Ensure user and group were not created
            user_info = self._get_user_info(test_user)
            self.assertIsNone(user_info, "User should not be created by ro_admin user")
        finally:
            # Cleanup
            self.run_cbq_query(f"DROP USER {ro_user}")

            # INSERT_YOUR_CODE

    def test_alter_user_password(self):
        user = "alter_user1"
        password = "oldpass123"
        new_password = "newpass456"
        self.assertGreaterEqual(len(password), 6)
        self.assertGreaterEqual(len(new_password), 6)
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        # Change password
        self.run_cbq_query(f'ALTER USER {user} PASSWORD "{new_password}"')
        # Try to run a query as the user with new password
        self.run_cbq_query(f"SELECT 1", username=user, password=new_password)
        self.run_cbq_query(f"DROP USER {user}")

    def test_alter_user_name(self):
        user = "alter_user2"
        password = "pass234"
        name = "Original Name"
        new_name = "Updated Name"
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}" WITH "{name}"')
        # Change name
        self.run_cbq_query(f'ALTER USER {user} WITH "{new_name}"')
        info = self._get_user_info(user)
        self.assertIsNotNone(info)
        self.assertEqual(info.get("name"), new_name)
        self.run_cbq_query(f"DROP USER {user}")

    def test_alter_user_group(self):
        user = "alter_user3"
        password = "pass345"
        group1 = "alter_group1"
        group2 = "alter_group2"
        self.run_cbq_query(f'CREATE GROUP {group1} NO ROLES')
        self.run_cbq_query(f'CREATE GROUP {group2} NO ROLES')
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        # Set single group
        self.run_cbq_query(f'ALTER USER {user} GROUP {group1}')
        info = self._get_user_info(user)
        self.assertEqual(info.get("groups"), [group1])
        # Set multiple groups
        self.run_cbq_query(f'ALTER USER {user} GROUPS {group1}, {group2}')
        info = self._get_user_info(user)
        self.assertCountEqual(info.get("groups"), [group1, group2])
        # Clear groups
        self.run_cbq_query(f'ALTER USER {user} NO GROUPS')
        info = self._get_user_info(user)
        self.assertEqual(info.get("groups"), [])
        self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f"DROP GROUP {group1}")
        self.run_cbq_query(f"DROP GROUP {group2}")

    def test_alter_user_all_options(self):
        user = "alter_user4"
        password = "pass456"
        new_password = "pass789"
        name = "Name1"
        new_name = "Name2"
        group = "alter_group3"
        self.run_cbq_query(f'CREATE GROUP {group} NO ROLES')
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}" WITH "{name}"')
        # All options at once
        self.run_cbq_query(
            f'ALTER USER {user} PASSWORD "{new_password}" WITH "{new_name}" GROUP {group}'
        )
        info = self._get_user_info(user)
        self.assertEqual(info.get("name"), new_name)
        self.assertEqual(info.get("groups"), [group])
        # Try to run a query as the user with new password
        self.run_cbq_query(f"SELECT 1", username=user, password=new_password)
        self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f"DROP GROUP {group}")

    def test_alter_user_group_options_exclusive(self):
        user = "alter_user5"
        password = "pass567"
        group = "alter_group4"
        self.run_cbq_query(f'CREATE GROUP {group} NO ROLES')
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        # GROUP and GROUPS together should fail
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER USER {user} GROUP {group} GROUPS {group}')
        # GROUP and NO GROUPS together should fail
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER USER {user} GROUP {group} NO GROUPS')
        # GROUPS and NO GROUPS together should fail
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER USER {user} GROUPS {group} NO GROUPS')
        self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f"DROP GROUP {group}")

    def test_alter_user_duplicate_options(self):
        user = "alter_user6"
        password = "pass678"
        group = "alter_group5"
        self.run_cbq_query(f'CREATE GROUP {group} NO ROLES')
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}"')
        # Duplicate PASSWORD option
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER USER {user} PASSWORD "{password}" PASSWORD "otherpass"')
        # Duplicate WITH option
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER USER {user} WITH "A" WITH "B"')
        # Duplicate GROUP option
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER USER {user} GROUP {group} GROUP {group}')
        # Duplicate GROUPS option
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER USER {user} GROUPS {group} GROUPS {group}')
        # Duplicate NO GROUPS option
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER USER {user} NO GROUPS NO GROUPS')
        self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f"DROP GROUP {group}")

    def test_alter_user_nonexistent(self):
        user = "alter_user_nonexistent"
        # Altering a non-existent user should fail
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER USER {user} PASSWORD "somepass"')

    def test_alter_group_with_and_roles(self):
        group = "alter_group1"
        user = "alter_user1"
        password = "pass789"
        # Create group with no roles and a description
        self.run_cbq_query(f'CREATE GROUP {group} NO ROLES')
        # Add a user to the group
        self.run_cbq_query(f'CREATE USER {user} PASSWORD "{password}" GROUP {group}')
        # Alter group: set description and a single role
        self.run_cbq_query(f'ALTER GROUP {group} WITH "Altered description" ROLE query_select ON `{self.bucket}`')
        # Verify group info
        group_info_res = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group}'")
        self.assertTrue(group_info_res['results'], f"Group {group} should exist in system:group_info")
        group_info = group_info_res['results'][0]['group_info']
        self.assertEqual(group_info.get("description"), "Altered description")
        roles = group_info.get("roles", [])
        self.assertEqual(len(roles), 1)
        self.assertEqual(roles[0].get("role"), "select")
        self.assertEqual(roles[0].get("bucket_name"), self.bucket)
        # Alter group: set multiple roles (should replace previous roles)
        self.run_cbq_query(
            f'ALTER GROUP {group} ROLES query_insert ON `{self.bucket}`, query_use_sequential_scans ON `{self.bucket}`'
        )
        group_info_res2 = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group}'")
        group_info2 = group_info_res2['results'][0]['group_info']
        roles2 = group_info2.get("roles", [])
        role_names = set(r.get("role") for r in roles2)
        self.assertEqual(role_names, {"insert", "query_use_sequential_scans"})
        # Also check user can run select query (should fail) and insert (should succeed)
        # Try SELECT as user (should fail, no select privilege)
        with self.assertRaises(CBQError):
            self.run_cbq_query(f"SELECT * FROM `{self.bucket}` LIMIT 1", username=user, password=password)
        # Try INSERT as user (should succeed)
        try:
            self.run_cbq_query(
                f"INSERT INTO `{self.bucket}` (KEY, VALUE) VALUES ('test_alter_group_key', {{'val': 1}})",
                username=user,
                password=password
            )
        finally:
            # Clean up inserted doc if possible
            try:
                self.run_cbq_query(
                    f"DELETE FROM `{self.bucket}` WHERE META().id = 'test_alter_group_key'"
                )
            except Exception:
                pass
        # Alter group: set NO ROLES (should remove all roles)
        self.run_cbq_query(f'ALTER GROUP {group} NO ROLES')
        group_info_res3 = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group}'")
        group_info3 = group_info_res3['results'][0]['group_info']
        self.assertEqual(group_info3.get("roles", []), [])
        # Cleanup
        self.run_cbq_query(f"DROP USER {user}")
        self.run_cbq_query(f"DROP GROUP {group}")

    def test_alter_group_duplicate_options(self):
        group = "alter_group2"
        self.run_cbq_query(f'CREATE GROUP {group} NO ROLES')
        # Duplicate WITH option
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER GROUP {group} WITH "A" WITH "B"')
        # Duplicate ROLE option
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER GROUP {group} ROLE query_select ON `{self.bucket}` ROLE query_insert ON `{self.bucket}`')
        # Duplicate ROLES option
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER GROUP {group} ROLES query_select ON `{self.bucket}` ROLES query_insert ON `{self.bucket}`')
        # Duplicate NO ROLES option
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER GROUP {group} NO ROLES NO ROLES')
        self.run_cbq_query(f"DROP GROUP {group}")

    def test_alter_group_nonexistent(self):
        group = "alter_group_nonexistent"
        # Altering a non-existent group should fail
        with self.assertRaises(CBQError):
            self.run_cbq_query(f'ALTER GROUP {group} WITH "desc"')
            # INSERT_YOUR_CODE

    def test_grant_and_revoke_role_on_group(self):
        """
        Test GRANT and REVOKE role on group using various syntaxes.
        """
        group1 = "grant_group1"
        group2 = "grant_group2"
        user1 = "grant_user1"
        user2 = "grant_user2"
        password = "grantpass1"
        self.assertGreaterEqual(len(password), 6, "Password must be at least 6 characters long")

        # Create groups
        self.run_cbq_query(f'CREATE GROUP {group1} NO ROLES')
        self.run_cbq_query(f'CREATE GROUP {group2} NO ROLES')

        # Create users in groups
        self.run_cbq_query(f'CREATE USER {user1} PASSWORD "{password}" GROUP {group1}')
        self.run_cbq_query(f'CREATE USER {user2} PASSWORD "{password}" GROUP {group2}')

        try:
            # Grant query_select on bucket to group1 using GROUP
            self.run_cbq_query(f'GRANT query_select ON `{self.bucket}` TO GROUP {group1}')
            group_info = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group1}'")['results'][0]['group_info']
            roles = group_info.get("roles", [])
            self.assertTrue(any(r.get("role") == "select" and r.get("bucket_name") == self.bucket for r in roles),
                            f"{group1} should have select on {self.bucket}")

            # Grant query_insert on bucket to both groups using GROUPS
            self.run_cbq_query(f'GRANT query_insert ON `{self.bucket}` TO GROUPS {group1}, {group2}')
            group_info1 = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group1}'")['results'][0]['group_info']
            group_info2 = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group2}'")['results'][0]['group_info']
            roles1 = group_info1.get("roles", [])
            roles2 = group_info2.get("roles", [])
            self.assertTrue(any(r.get("role") == "insert" and r.get("bucket_name") == self.bucket for r in roles1),
                            f"{group1} should have insert on {self.bucket}")
            self.assertTrue(any(r.get("role") == "insert" and r.get("bucket_name") == self.bucket for r in roles2),
                            f"{group2} should have insert on {self.bucket}")

            # Grant multiple roles at once
            self.run_cbq_query(f'GRANT query_update, query_delete ON `{self.bucket}` TO GROUP {group1}')
            group_info = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group1}'")['results'][0]['group_info']
            roles = group_info.get("roles", [])
            self.assertTrue(any(r.get("role") == "update" and r.get("bucket_name") == self.bucket for r in roles),
                            f"{group1} should have update on {self.bucket}")
            self.assertTrue(any(r.get("role") == "delete" and r.get("bucket_name") == self.bucket for r in roles),
                            f"{group1} should have delete on {self.bucket}")

            # Revoke query_insert from group1 using GROUP
            self.run_cbq_query(f'REVOKE query_insert ON `{self.bucket}` FROM GROUP {group1}')
            group_info = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group1}'")['results'][0]['group_info']
            roles = group_info.get("roles", [])
            self.assertFalse(any(r.get("role") == "insert" and r.get("bucket_name") == self.bucket for r in roles),
                             f"{group1} should not have insert on {self.bucket}")

            # Revoke query_insert from group2 using GROUPS
            self.run_cbq_query(f'REVOKE query_insert ON `{self.bucket}` FROM GROUPS {group2}')
            group_info2 = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group2}'")['results'][0]['group_info']
            roles2 = group_info2.get("roles", [])
            self.assertFalse(any(r.get("role") == "insert" and r.get("bucket_name") == self.bucket for r in roles2),
                             f"{group2} should not have insert on {self.bucket}")

            # Revoke multiple roles at once
            self.run_cbq_query(f'REVOKE query_update, query_delete ON `{self.bucket}` FROM GROUP {group1}')
            group_info = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group1}'")['results'][0]['group_info']
            roles = group_info.get("roles", [])
            self.assertFalse(any(r.get("role") == "update" and r.get("bucket_name") == self.bucket for r in roles),
                             f"{group1} should not have update on {self.bucket}")
            self.assertFalse(any(r.get("role") == "delete" and r.get("bucket_name") == self.bucket for r in roles),
                             f"{group1} should not have delete on {self.bucket}")

            # Grant role without ON (should grant on all buckets, but for test, just check no error)
            self.run_cbq_query(f'GRANT query_system_catalog TO GROUP {group1}')
            group_info = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group1}'")['results'][0]['group_info']
            self.assertTrue(any(r.get("role") == "query_system_catalog" for r in group_info.get("roles", [])),
                            f"{group1} should have query_system_catalog role")

            # Revoke role without ON
            self.run_cbq_query(f'REVOKE query_system_catalog FROM GROUP {group1}')
            group_info = self.run_cbq_query(f"SELECT * FROM system:group_info WHERE id = '{group1}'")['results'][0]['group_info']
            self.assertFalse(any(r.get("role") == "query_system_catalog" for r in group_info.get("roles", [])),
                             f"{group1} should not have query_system_catalog role")

        finally:
            # Cleanup users and groups
            self.run_cbq_query(f"DROP USER {user1}")
            self.run_cbq_query(f"DROP USER {user2}")
            self.run_cbq_query(f"DROP GROUP {group1}")
            self.run_cbq_query(f"DROP GROUP {group2}")
