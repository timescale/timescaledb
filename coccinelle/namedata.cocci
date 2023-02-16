// NameData is a fixed-size type of 64 bytes. Using strlcpy to copy data into a
// NameData struct can cause problems because any data that follows the initial
// null-terminated string will also be part of the data.

@rule_var_decl_struct@
symbol NAMEDATALEN;
identifier I1, I2;
@@
struct I1
{
  ...
- char I2[NAMEDATALEN];
+ /* You are declaring a char of length NAMEDATALEN, please consider using NameData instead. */
+ NameData I2;
  ...
}

@rule_namedata_strlcpy@
expression E1, E2;
symbol NAMEDATALEN;
@@
- strlcpy(E1, E2, NAMEDATALEN);
+ /* You are using strlcpy with NAMEDATALEN, please consider using NameData and namestrcpy instead. */
+ namestrcpy(E1, E2);
