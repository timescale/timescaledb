# Submodule Licensing and Initialization #

## Loading and Activation ##

We link module loading and activation to the license GUC itself.
We have a single GUC, the license, and load submodules based on
what capabilities the license enables, i.e., an `apache` license-key does
not load this module, while `timescale` key does. This
ensures that the loader "does the right thing" with respect to the license, and
a user cannot accidentally activate features they aren't licensed to use.

The actual loading and activation is done through `check` and `assign` hooks on
the license GUC. On `check` we validate the license type 
and on `assign` we set the capabilities-struct in this module,
if needed. The `check` and `assign` functions can be found in
[`license_guc.c/h`](/src/license_guc.c) in the Apache-Licensed src.

### Cross License Functions ###

To enable binaries which only contain Apache-Licensed code,
we dynamically link in Timescale-Licensed code on license activation,
and handle all function calls into the module via function pointers.

The registry in `ts_cm_functions` of type `CrossModuleFunctions` (declared in
[`cross_module_fn.h`](/src/cross_module_fn.h) and defined in
[`cross_module_fn.c`](/src/cross_module_fn.c)) stores all of the cross-module
functions.

To add a new cross-module function you must:

  - Add a struct member `CrossModuleFunctions.<function name>`.
  - Add default function to `ts_cm_functions_default` that will be called from the Apache version, usually this function should just call `error_no_default_fn`. **NOTE** Due to function-pointer casting rules, the default function must have the exact same signature as the function pointer; you may _not_ cast another function pointer of another type.
  - Add the overriding function to `tsl_cm_functions`in `init.c` in this module.

To call a cross-module functions use `ts_cm_functions-><function name>(args)`.
