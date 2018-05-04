# erloci_nif
Oracle OCI driver using dirty NIF

Requires Erlang/OTP 20 or later with full dirty nif support.

All remote oci operations are run within the pool of ERL_NIF_DIRTY_JOB_IO_BOUND threads

Status: Early prototype

Todo:

- [ ] Tidy up error handling
- [ ] a bunch more OCI operations
- [ ] cursor support
- [ ] custom datatype support
- [ ] implement cleanup in the destructors for the resource type handles
- [ ] Test Suite
- [ ] Nice front end API
- [ ] much more
