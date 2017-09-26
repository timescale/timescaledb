--Disable the event trigger during updates. Two reasons:
---1- Prevents the old extension .so being loaded during upgrade to process the event trigger.
---2- Probably the right thing to do anyway since you don't necessarly know which version of the trigger will be fired during upgrade.
ALTER EVENT TRIGGER timescaledb_ddl_command_end DISABLE;
