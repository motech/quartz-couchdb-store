package org.motechproject.quartz;

import org.codehaus.jackson.annotate.JsonProperty;
import org.ektorp.support.TypeDiscriminator;
import org.quartz.DateBuilder;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;

import java.util.TimeZone;

@TypeDiscriminator("CouchDbTrigger")
public class CouchDbCalendarIntervalTrigger extends CouchDbTrigger<CalendarIntervalTriggerImpl> {


    private CouchDbCalendarIntervalTrigger() {
        this(new CalendarIntervalTriggerImpl());
    }

    public CouchDbCalendarIntervalTrigger(CalendarIntervalTriggerImpl trigger, CouchDbTriggerState triggerState) {
        super(trigger, triggerState);
    }

    public CouchDbCalendarIntervalTrigger(CalendarIntervalTriggerImpl trigger) {
        this(trigger, CouchDbTriggerState.WAITING);
    }

    @JsonProperty("repeat_interval")
    public int getRepeatInterval() {
        return getBaseTrigger().getRepeatInterval();
    }

    @JsonProperty("repeat_interval")
    public void setRepeatInterval(int repeatInterval) {
        getBaseTrigger().setRepeatInterval(repeatInterval);
    }

    @JsonProperty("repeat_interval_unit")
    public DateBuilder.IntervalUnit getRepeatIntervalUnit() {
        return getBaseTrigger().getRepeatIntervalUnit();
    }

    @JsonProperty("repeat_interval_unit")
    public void setRepeatIntervalUnit(DateBuilder.IntervalUnit unit) {
        getBaseTrigger().setRepeatIntervalUnit(unit);
    }

    @JsonProperty("times_triggered")
    public int getTimesTriggered() {
        return getBaseTrigger().getTimesTriggered();
    }

    @JsonProperty("times_triggered")
    public void setTimesTriggered(int timesTriggered) {
        getBaseTrigger().setTimesTriggered(timesTriggered);
    }

    @JsonProperty("timezone")
    public String getTimezoneId() {
        return getBaseTrigger().getTimeZone() != null ? getBaseTrigger().getTimeZone().getID() : null;
    }

    @JsonProperty("timezone")
    public void setTimezoneId(String timezoneId) {
        if (timezoneId != null) {
            getBaseTrigger().setTimeZone(TimeZone.getTimeZone(timezoneId));
        }
    }
}
