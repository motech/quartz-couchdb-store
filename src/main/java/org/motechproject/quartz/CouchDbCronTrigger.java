package org.motechproject.quartz;

import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonProperty;
import org.ektorp.support.TypeDiscriminator;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.text.ParseException;
import java.util.TimeZone;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@TypeDiscriminator("CouchDbTrigger")
public class CouchDbCronTrigger extends CouchDbTrigger<CronTriggerImpl> {

    private Logger log = Logger.getLogger(CouchDbCronTrigger.class);

    private CouchDbCronTrigger() {
        this(new CronTriggerImpl());
    }

    public CouchDbCronTrigger(CronTriggerImpl trigger, CouchDbTriggerState triggerState) {
        super(trigger, triggerState);
    }


    public CouchDbCronTrigger(CronTriggerImpl trigger) {
        this(trigger, CouchDbTriggerState.WAITING);
    }


    @JsonProperty("cron_expression")
    public String getCronExpression() {
        return getTrigger().getCronExpression();
    }

    @JsonProperty("cron_expression")
    public void setCronExpression(String expr) throws ParseException {
        getTrigger().setCronExpression(expr);
    }

    @JsonProperty("cron_timezone")
    public String getTimezoneId() {
        return getTrigger().getTimeZone() != null ? getTrigger().getTimeZone().getID() : null;
    }

    @JsonProperty("cron_timezone")
    public void setTimezoneId(String timezoneId) {
        if (timezoneId != null) {
            getTrigger().setTimeZone(TimeZone.getTimeZone(timezoneId));
        }
    }

}
