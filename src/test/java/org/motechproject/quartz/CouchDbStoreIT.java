package org.motechproject.quartz;

import org.junit.Before;
import org.junit.Test;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.impl.calendar.WeeklyCalendar;
import org.quartz.impl.triggers.SimpleTriggerImpl;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.motechproject.quartz.IdRandomizer.id;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class CouchDbStoreIT {

    CouchDbStore couchdbStore;

    @Before
    public void setup() throws Exception, CouchDbJobStoreException {
        couchdbStore = new CouchDbStore();
        couchdbStore.setProperties("/couchdb.properties");
        couchdbStore.clearAllSchedulingData();
    }

    @Test
    public void shouldDeleteAllJobsTriggersAndCalendars() throws JobPersistenceException {
        JobDetail job = newJob(JobListener.class)
                .withIdentity(id("fooid"), id("bargroup"))
                .usingJobData("foo", "bar")
                .build();
        couchdbStore.storeJob(job, false);

        Calendar testCalendar = new WeeklyCalendar();
        String calendarName = id("weeklyCalendar");
        couchdbStore.storeCalendar(calendarName, testCalendar, false, false);

        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(id("fuuid"), id("borgroup"))
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .modifiedByCalendar(calendarName)
                .build();
        couchdbStore.storeTrigger(trigger, false);

        couchdbStore.clearAllSchedulingData();

        assertEquals(0, couchdbStore.getNumberOfJobs());
        assertEquals(0, couchdbStore.getNumberOfCalendars());
        assertEquals(0, couchdbStore.getNumberOfTriggers());
        assertEquals(0, couchdbStore.getJobGroupNames().size());
        assertEquals(0, couchdbStore.getCalendarNames().size());
    }
}
