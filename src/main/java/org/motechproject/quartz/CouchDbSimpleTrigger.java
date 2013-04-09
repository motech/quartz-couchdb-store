package org.motechproject.quartz;

import org.codehaus.jackson.annotate.JsonProperty;
import org.ektorp.support.TypeDiscriminator;
import org.quartz.impl.triggers.SimpleTriggerImpl;

@TypeDiscriminator("CouchDbTrigger")
public class CouchDbSimpleTrigger extends CouchDbTrigger<SimpleTriggerImpl> {


    private CouchDbSimpleTrigger() {
        this(new SimpleTriggerImpl());
    }

    public CouchDbSimpleTrigger(SimpleTriggerImpl trigger, CouchDbTriggerState triggerState) {
        super(trigger, triggerState);
    }

    public CouchDbSimpleTrigger(SimpleTriggerImpl trigger) {
        this(trigger, CouchDbTriggerState.WAITING);
    }

    @JsonProperty("repeat_count")
    public int getRepeatCount() {
        return getTrigger().getRepeatCount();
    }

    @JsonProperty("repeat_count")
    public void setRepeatCount(int repeatCount) {
        getTrigger().setRepeatCount(repeatCount);
    }

    @JsonProperty("repeat_interval")
    public long getRepeatInterval() {
        return getTrigger().getRepeatInterval();
    }

    @JsonProperty("repeat_interval")
    public void setRepeatInterval(long repeatInterval) {
        getTrigger().setRepeatInterval(repeatInterval);
    }

    @JsonProperty("times_triggered")
    public int getTimesTriggered() {
        return getTrigger().getTimesTriggered();
    }

    @JsonProperty("times_triggered")
    public void setTimesTriggered(int timesTriggered) {
        getTrigger().setTimesTriggered(timesTriggered);
    }
}
