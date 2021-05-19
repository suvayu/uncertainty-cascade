Uncertainty cascade
===================

This package does mundane tasks related to the uncertainty propagation (6.2) task in the SENTINEL project

- preprocess DESTinEE demand profiles for Calliope.  All data quirks
  are documented with the "NOTE:" token in comments; use something
  like ``grep -F "NOTE:" -R`` to find them.
- generate different scenario configurations for Calliope
- extract metrics from different Calliope runs and aggregate them for
  comparison.
