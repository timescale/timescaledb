# Suppressions for Clang Sanitizers #

This folder contains [supression files](https://clang.llvm.org/docs/SanitizerSpecialCaseList.html) for
running timescale using Clang's [AddressSanitizer](https://clang.llvm.org/docs/AddressSanitizer.html)
and [UndefinedBehaviorSanitizer](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html), which
we use as part of timescale's regission suite. There are a few places OSs have UB and where postgres
has benign memory leaks, in order to run these sanitizers, we suppress these warning.

For ease of use, we provide a script in [/scripts/test_sanitizers.sh] to run our regression tests with
sanitizers enabled.
