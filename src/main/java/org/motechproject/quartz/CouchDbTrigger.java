package org.motechproject.quartz;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.AbstractTrigger;

import java.util.Date;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "triggerType")
@JsonSubTypes({
    @JsonSubTypes.Type(value = CouchDbSimpleTrigger.class, name = "simple"),
    @JsonSubTypes.Type(value = CouchDbCronTrigger.class, name = "cron"),
    @JsonSubTypes.Type(value = CouchDbCalendarIntervalTrigger.class, name = "calendar")
})
public abstract class CouchDbTrigger<T extends AbstractTrigger> {

    private T trigger;

    private String revision;
    private String type;
    private CouchDbTriggerState state;

    private Date startTime;
    private Date endTime;

    public CouchDbTrigger(T trigger, CouchDbTriggerState state) {
        this.type = "CouchDbTrigger";
        this.trigger = trigger;
        this.state = state;
    }

    public CouchDbTrigger(T trigger) {
        this(trigger, CouchDbTriggerState.WAITING);
    }

    @JsonProperty("type")
    public String getType() {
        return this.type;
    }

    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty("_id")
    public String getId() {
        return toId(getGroup(), getName());
    }

    public static String toId(String group, String name) {
        return "trigger:" + group + "-" + name;
    }

    @JsonProperty("_id")
    public void setId(String id) {
        // id is derived dont set it
    }

    @JsonProperty("_rev")
    public String getRevision() {
        return revision;
    }

    @JsonProperty("_rev")
    public void setRevision(String revision) {
        this.revision = revision;
    }


    @JsonProperty("trigger_name")
    public String getName() {
        return trigger.getName();
    }

    @JsonProperty("trigger_name")
    public void setName(String name) {
        trigger.setName(name);
    }

    @JsonProperty("trigger_group")
    public String getGroup() {
        return trigger.getGroup();
    }

    @JsonProperty("trigger_group")
    public void setGroup(String group) {
        trigger.setGroup(group);
    }

    @JsonProperty("job_name")
    public String getJobName() {
        return trigger.getJobName();
    }

    @JsonProperty("job_name")
    public void setJobName(String jobName) {
        trigger.setJobName(jobName);
    }

    @JsonProperty("job_group")
    public String getJobGroup() {
        return trigger.getJobGroup();
    }

    @JsonProperty("job_group")
    public void setJobGroup(String jobGroup) {
        trigger.setJobGroup(jobGroup);
    }

    @JsonProperty("description")
    public String getDescription() {
        return trigger.getDescription();
    }


    @JsonProperty("description")
    public void setDescription(String description) {
        trigger.setDescription(description);
    }

    @JsonProperty("state")
    public CouchDbTriggerState getState() {
        return state;
    }

    @JsonProperty("state")
    public void setState(CouchDbTriggerState state) {
        this.state = state;
    }

    @JsonProperty("next_fire_time")
    public Date getNextFireTime() {
        return trigger.getNextFireTime();
    }

    @JsonProperty("next_fire_time")
    public void setNextFireTime(Date nextFireTime) {
        trigger.setNextFireTime(nextFireTime);
    }

    @JsonProperty("previous_fire_time")
    public Date getPreviousFireTime() {
        return trigger.getPreviousFireTime();
    }

    @JsonProperty("previous_fire_time")
    public void setPreviousFireTime(Date previousFireTime) {
        trigger.setPreviousFireTime(previousFireTime);
    }

    @JsonProperty("priority")
    public Integer getPriorityValue() {
        return trigger.getPriority();
    }

    @JsonProperty("priority")
    public void setPriorityValue(Integer priority) {
        trigger.setPriority(priority);
    }

    @JsonProperty("start_time")
    public Date getStartTime() {
        return trigger.getStartTime();
    }

    @JsonProperty("start_time")
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
        trigger.setStartTime(this.startTime);
        setEndTimeIfAlreadyDeserialized();
    }

    @JsonProperty("end_time")
    public Date getEndTime() {
        return trigger.getEndTime();
    }

    /* if trigger.setEndTime() is called before start time is deserialized by jackson,
       it complains that end time must be before start time; workaround to ensure setEndTime()
       is called only after both startTime and endTime have been deserialized
     */
    @JsonProperty("end_time")
    public void setEndTime(Date endTime) {
        this.endTime = endTime;
        if (this.startTime != null) {
            setEndTimeIfAlreadyDeserialized();
        }
    }

    private void setEndTimeIfAlreadyDeserialized() {
        if (this.endTime != null) {
            trigger.setEndTime(this.endTime);
            this.endTime = null;
        }
    }

    @JsonProperty("calendar_name")
    public void setCalendarName(String calendarName) {
        trigger.setCalendarName(calendarName);
    }

    @JsonProperty("calendar_name")
    public String getCalendarName() {
        return trigger.getCalendarName();
    }

    @JsonProperty("misfire_instruction")
    public void setMisfireInstructionValue(Integer misfireInstruction) {
        trigger.setMisfireInstruction(misfireInstruction);
    }

    @JsonProperty("misfire_instruction")
    public Integer getMisfireInstructionValue() {
        return trigger.getMisfireInstruction();
    }

    @JsonProperty("job_data")
    public JobDataMap getJobDataMap() {
        return trigger.getJobDataMap();
    }

    @JsonProperty("job_data")
    public void setJobDataMap(JobDataMap jobDataMap) {
        trigger.setJobDataMap(jobDataMap);
    }

    @JsonIgnore
    public T getTrigger() {
        return trigger;
    }

    @JsonIgnore
    public T getBaseTrigger() {
        return trigger;
    }

    @JsonIgnore
    public TriggerKey getKey() {
        return trigger.getKey();
    }

    @JsonIgnore
    public JobKey getJobKey() {
        return trigger.getJobKey();
    }

    @JsonIgnore
    public void triggered(Calendar calendar) {
        trigger.triggered(calendar);
    }

    @JsonIgnore
    public void updateWithNewCalendar(Calendar calendar, int misfireThreshold) {
        trigger.updateWithNewCalendar(calendar, misfireThreshold);
    }
}
