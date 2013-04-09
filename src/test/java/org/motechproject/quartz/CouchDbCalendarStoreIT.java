package org.motechproject.quartz;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.quartz.Calendar;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.TriggerKey;
import org.quartz.impl.calendar.CronCalendar;
import org.quartz.impl.calendar.HolidayCalendar;
import org.quartz.impl.calendar.WeeklyCalendar;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static ch.lambdaj.Lambda.extract;
import static ch.lambdaj.Lambda.on;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.motechproject.quartz.IdRandomizer.id;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;


public class CouchDbCalendarStoreIT {

    CouchDbStore couchdbStore;

    @Before
    public void setup() throws Exception, CouchDbJobStoreException {
        couchdbStore = new CouchDbStore();
        couchdbStore.setProperties("/couchdb.properties");
        couchdbStore.clearAllSchedulingData();
    }

    @Test
    public void shouldStoreAndRetrieveCalendar() throws Exception {
        DateTime now = new DateTime();
        HolidayCalendar testCalendar = new HolidayCalendar();
        testCalendar.addExcludedDate(now.plusDays(1).toDate());

        String calendarName = id("testCalendar");
        couchdbStore.storeCalendar(calendarName, testCalendar, false, false);

        Calendar dbCalendar = couchdbStore.retrieveCalendar(calendarName);
        assertEquals(testCalendar.getClass(), dbCalendar.getClass());
        assertEquals(testCalendar.getExcludedDates(), ((HolidayCalendar) dbCalendar).getExcludedDates());
    }

    @Test
    public void shouldReplaceExistingCalendar() throws Exception {
        HolidayCalendar testCalendar = new HolidayCalendar();
        testCalendar.addExcludedDate(new Date());

        String calendarName = id("testCalendar");
        couchdbStore.storeCalendar(calendarName, testCalendar, false, false);
        couchdbStore.storeCalendar(calendarName, new HolidayCalendar(), true, false);

        HolidayCalendar dbCalendar = (HolidayCalendar) couchdbStore.retrieveCalendar(calendarName);
        assertEquals(0, dbCalendar.getExcludedDates().size());
    }

    @Test(expected = ObjectAlreadyExistsException.class)
    public void shouldNotReplaceExistingCalendar() throws Exception {
        couchdbStore.storeCalendar("testCalendar", new HolidayCalendar(), false, false);
        couchdbStore.storeCalendar("testCalendar", new HolidayCalendar(), false, false);
    }

    @Test
    public void shouldNotUpdateExistingTriggers() throws Exception {
        String triggerName = id("triggerName");
        String triggerGroup = id("triggerGroup");
        String calendarName = id("testCalendar");

        couchdbStore.storeCalendar(calendarName, new HolidayCalendar(), false, false);

        final DateTime startTime = new DateTime().plusDays(1);
        OperableTrigger trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, triggerGroup)
                .forJob(JobKey.jobKey(id("jobName"), id("jobGroup")))
                .startAt(startTime.toDate())
                .withSchedule(simpleSchedule()
                        .withIntervalInHours(24)
                        .repeatForever())
                .modifiedByCalendar(calendarName)
                .build();
        trigger.computeFirstFireTime(couchdbStore.retrieveCalendar(calendarName));
        couchdbStore.storeTrigger(trigger, false);

        OperableTrigger triggerWithEmptyCalendar = couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName, triggerGroup));
        assertEquals(startTime.toDate(), triggerWithEmptyCalendar.getNextFireTime());

        HolidayCalendar testCalendar = new HolidayCalendar();
        testCalendar.addExcludedDate(startTime.toDate());
        couchdbStore.storeCalendar(calendarName, testCalendar, true, false);
        assertEquals(startTime.toDate(), triggerWithEmptyCalendar.getNextFireTime());

        couchdbStore.storeCalendar(calendarName, testCalendar, true, true);
        OperableTrigger triggerWithTestCalendar = couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName, triggerGroup));
        assertEquals(startTime.plusDays(1).toDate(), triggerWithTestCalendar.getNextFireTime());
    }

    @Test
    public void shouldRemoveCalendar() throws JobPersistenceException {
        HolidayCalendar testCalendar = new HolidayCalendar();

        String calendarName = id("testCalendar");
        couchdbStore.storeCalendar(calendarName, testCalendar, false, false);

        assertTrue(couchdbStore.removeCalendar(calendarName));

        assertNull(couchdbStore.retrieveCalendar(calendarName));
    }

    @Test
    public void shouldGetAllCalendarNames() throws JobPersistenceException, ParseException {
        Calendar testCalendar = new WeeklyCalendar();
        String calendarName1 = id("weeklyCalendar");
        couchdbStore.storeCalendar(calendarName1, testCalendar, false, false);

        Calendar cronCalendar = new CronCalendar("1 1 1 * * ?");
        String calendarName2 = id("cronCalendar");
        couchdbStore.storeCalendar(calendarName2, cronCalendar, false, false);

        List<String> calendarNames = couchdbStore.getCalendarNames();
        assertTrue(calendarNames.size()>0);
        assertTrue(calendarNames.contains(calendarName1));
        assertTrue(calendarNames.contains(calendarName2));
    }

    @Test
    public void shouldGetAllCalendars() throws JobPersistenceException, ParseException {
        Calendar testCalendar = new WeeklyCalendar();
        String calendarName1 = id("weeklyCalendar");
        couchdbStore.storeCalendar(calendarName1, testCalendar, false, false);

        Calendar cronCalendar = new CronCalendar("1 1 1 * * ?");
        String calendarName2 = id("cronCalendar");
        couchdbStore.storeCalendar(calendarName2, cronCalendar, false, false);

        Calendar anotherCalendar = new CronCalendar("1 1 1 * * ?");
        String calendarName3 = id("anotherCalendar");
        couchdbStore.storeCalendar(calendarName3, anotherCalendar, false, false);

        List<CouchDbCalendar> calendars = couchdbStore.getCalendarStore().getCalendars(asList(calendarName1, calendarName3));
        List<String> calendarNames = extract(calendars, on(CouchDbCalendar.class).getName());
        assertEquals(2, calendars.size());
        assertTrue(calendarNames.contains(calendarName1));
        assertTrue(calendarNames.contains(calendarName3));
    }
}
