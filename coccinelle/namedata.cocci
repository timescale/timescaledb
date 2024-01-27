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
identifier I1;
expression E1;
symbol NAMEDATALEN;
@@
- strlcpy(I1, E1, NAMEDATALEN);
+ /* You are using strlcpy with NAMEDATALEN, please consider using NameData and namestrcpy instead. */
+ namestrcpy(I1, E1);

@rule_namedata_memcpy@
expression E1, E2;
symbol NAMEDATALEN;
@@
- memcpy(E1, E2, NAMEDATALEN);
+ /* You are using memcpy with NAMEDATALEN, please consider using NameData and namestrcpy instead. */
+ namestrcpy(E1, E2);

@@
typedef NameData;
NameData E;
@@
- E.data
+ /* Use NameStr rather than accessing data member directly */
+ NameStr(E)

@@
NameData *E;
@@
- E->data
+ /* Use NameStr rather than accessing data member directly */
+ NameStr(*E)

@@
typedef Name;
Name E;
@@
- E->data
+ /* Use NameStr rather than accessing data member directly */
+ NameStr(*E)
