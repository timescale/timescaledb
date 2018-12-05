# Submodule Licensing and Initialization #

## Loading and Activation ##

We link module loading and activation to the license key GUC itself.
While keeping multiple GUCs, e.g., one for module loading and one licensing,
is tempting, it leads to hairy edge cases when the GUCs disagree.
Instead, we have a single GUC, the license key, and load submodules based on
what capabilities the license key enables, i.e., an `ApacheOnly` license-key does
not load this module, while an `Enterprise` or `Community` key does. This
ensures that the loader "does the right thing" with respect to the license, and
a user cannot accidentally activate features they aren't licensed to use.

The actual loading and activation is done through `check` and `assign` hooks on
the license key GUC. On `check` we validate the license-key and deserialize it
if needed, and on `assign` we set the capabilities-struct in this module,
if needed. The `check` and `assign` functions can be found in
[`license_guc.c/h`](/src/license_guc.c) in the Apache-Licensed src, and the
validation code is in [`license.c/h`](./license.c) in this module.

### Cross License Functions ###

To enable binaries which only contain Apache-Licensed code,
we dynamically link in Timescale-Licensed code on license key activation,
and handle all function calls into the module via function pointers.

The registry in `ts_cm_functions` of type `CrossModuleFunctions` (decalared in
[`cross_module_fn.h`](/src/cross_module_fn.h) and defined in
[`cross_module_fn.c`](/src/cross_module_fn.c)) stores all of the cross-module
functions. To add a new cross-module function you must:
  - Add a struct member `CrossModuleFunctions.<function name>`.
  - Add default function to `ts_cm_functions_default` that will be called from the Apache version, usually this function should just call `error_no_default_fn`. **NOTE** Due to function-pointer casting rules, the default function must have the exact same signature as the function pointer; you may _not_ cast another function pointer of another type.
  - Add the overriding function to `tsl_cm_functions`in `init.c` in this module.

To call a cross-module functions use `ts_cm_functions-><function name>(args)`.

## License Validation ##

We want to keep all license verification code under the Timescale License;
code under the Apache License is freely modifieable, and if we include our
license-verification code there it may be possible to remove without violating
the license (_using_ the module without a valid license would still be in
violation).

Therefore we take a two stage approach to license validation:

1. In the Apache code we read the first character of the license to determine what type of license this is. This is enough to let us know if we need to load this module at all; ApacheOnly licenses can simply stop after this step.
2. If the license requires it (Community and Enterprise), we load this module and use `tsl_license_update_check` to check that the license is valid. This function returns whether a given license-string is a valid license, and optionally returns a deserialized representation of the licenses capabilities.

## License Format ##

In a simplified form the BNF for a valid license key is

```ABNF
LicenseKey = ApacheLicense / CommunityLicense / EnterpriseLicenseV1

ApacheLicense = "A" char*

CommunityLicense = "C" char*

EnterpriseLicense = "E" EnterpriseLicenseContents

EnterpriseLicenseContents = "1" <base64 encoded JSONLicenseInfo>
```

where a `JSONLicenseInfo` describes the capabilities granted by the license.
It is JSON corresponding to:

```JSON
{
    "id":"5D96CF28-87A6-48B5-AE8E-99CF851ABE0C", //uuid identifying the license
    "type":"trial", // either "trial" or "commercial"
    "start_time":"2018/10/01", //datetime at which the license was generated
    "end_time":"2018/10/31" //datetime at which the license expires
}
```

That is, an ApacheOnly license is an `'A'` followed by any string, the canonical
version of this license is `"ApacheOnly"`; a Community License is `'C'` followed
by any string, the canonical version being `'CommunityLicense'`; and an
Enterprise License is `"E1"` followed by a base64 encoded JSON containing the
capabilities the license enables.
