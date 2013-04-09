package org.motechproject.quartz;

import org.quartz.Trigger;

public enum CouchDbTriggerState {

    WAITING("WAITING"),
    ACQUIRED("ACQUIRED"),
    EXECUTING("EXECUTING"),
    COMPLETE("COMPLETE"),
    BLOCKED("BLOCKED"),
    ERROR("ERROR"),
    PAUSED("PAUSED"),
    PAUSED_BLOCKED("PAUSED_BLOCKED"),
    DELETED("DELETED");

    private String value;

    private CouchDbTriggerState(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public Trigger.TriggerState getQuartzTriggerState() {
        if (this == null) {
            return Trigger.TriggerState.NONE;
        }
        if (equals(DELETED)) {
            return Trigger.TriggerState.NONE;
        }
        if (equals(COMPLETE)) {
            return Trigger.TriggerState.COMPLETE;
        }
        if (equals(PAUSED)) {
            return Trigger.TriggerState.PAUSED;
        }
        if (equals(PAUSED_BLOCKED)) {
            return Trigger.TriggerState.PAUSED;
        }
        if (equals(ERROR)) {
            return Trigger.TriggerState.ERROR;
        }
        if (equals(BLOCKED)) {
            return Trigger.TriggerState.BLOCKED;
        }
        return Trigger.TriggerState.NORMAL;
    }
}
