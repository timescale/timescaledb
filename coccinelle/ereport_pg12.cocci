// Since PG 12.3 the ereport syntax changed. This coccinelle patch checks that the used 
// ereport calls work with PG < 12.3. 
//
// See postgres/postgres@a86715451653c730d637847b403b0420923956f7
//

@rule_1@
constant K1;
expression E1, E2;
@@

// We pass two or more expressions to ereport

+ /*
+  * Please enclose the auxiliary ereport arguments into parentheses for
+  * compatibility with PG 12. Example:
+  *
+  * ereport(ERROR, ( errmsg(...), errdetail(...) ) );
+  *                ^-----------add these---------^
+  *
+  * See https://github.com/postgres/postgres/commit/a86715451653c730d637847b403b0420923956f7
+  */
ereport(K1, E1, E2, ...);

