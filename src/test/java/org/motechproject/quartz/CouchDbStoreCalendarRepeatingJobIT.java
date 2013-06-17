package org.motechproject.quartz;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.motechproject.quartz.IdRandomizer.id;
import static org.quartz.CalendarIntervalScheduleBuilder.calendarIntervalSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class CouchDbStoreCalendarRepeatingJobIT {


    Scheduler scheduler;

    @Before
    public void setUp() throws Exception {
        StdSchedulerFactory stdSchedulerFactory = new StdSchedulerFactory("quartz.properties");
        scheduler = stdSchedulerFactory.getScheduler();
    }

    @Test
    public void shouldScheduleAndFireCalendarIntervalTrigger() throws InterruptedException, SchedulerException {
        try {
            scheduler.clear();
            Logger.getLogger("org.motechproject").setLevel(Level.ALL);
            scheduler.start();

            DateTime now = new DateTime();

            String subject = id("event");

            final String groupName = id("bargroup");
            JobDetail job = newJob(TestListener.class)
                    .withIdentity("foo", groupName)
                    .usingJobData("eventType", subject)
                    .build();

            CalendarIntervalTriggerImpl trigger = (CalendarIntervalTriggerImpl) newTrigger()
                    .withIdentity("fuu", groupName)
                    .forJob(JobKey.jobKey("foo", groupName))
                    .startAt(now.toDate())
                    .endAt(now.plusSeconds(3).toDate())
                    .withSchedule(calendarIntervalSchedule()
                            .withIntervalInSeconds(2))
                    .build();

            scheduler.scheduleJob(job, trigger);

            synchronized (TestListener.events()) {
                TestListener.events().wait(6000);
            }
            assertEquals(2, TestListener.events().size());

        } finally {
            scheduler.standby();
            Logger.getLogger("org.motechproject").setLevel(Level.WARN);
        }
    }
    public static class TestListener extends JobListener {

        static List<JobExecutionContext> events = new ArrayList<JobExecutionContext>();

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            synchronized (events) {
                events.add(context);
            }
        }

        public static List<JobExecutionContext> events() {
            return events;
        }
    }

}
