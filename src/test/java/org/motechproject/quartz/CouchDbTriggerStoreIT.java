package org.motechproject.quartz;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredResult;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static ch.lambdaj.Lambda.extract;
import static ch.lambdaj.Lambda.on;
import static java.util.Arrays.asList;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.motechproject.quartz.IdRandomizer.id;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class CouchDbTriggerStoreIT {

    CouchDbStore couchdbStore;

    @Before
    public void setup() throws Exception, CouchDbJobStoreException {
        couchdbStore = new CouchDbStore();
        couchdbStore.setProperties("/couchdb.properties");
        couchdbStore.clearAllSchedulingData();
    }

    @Test
    public void shouldStoreAndRetrieveTrigger() throws JobPersistenceException {
        final String triggerName = id("fuuid");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        assertNull(couchdbStore.retrieveTrigger(TriggerKey.triggerKey("something", "something")));
        assertEquals(new Date(2010, 10, 20), couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName, "borgroup")).getStartTime());
    }

    @Test
    public void shouldUpdateExistingTrigger() throws JobPersistenceException {
        final String triggerName = id("fuuid");
        final String jobId = id("fooid");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey(jobId, "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        SimpleTriggerImpl newTrigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey(jobId, "bargroup"))
                .startAt(new Date(2012, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(newTrigger, true);

        assertEquals(new Date(2012, 10, 20), couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName, "borgroup")).getStartTime());
    }

    @Test(expected = ObjectAlreadyExistsException.class)
    public void shouldNotUpdateExistingTrigger() throws JobPersistenceException {
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity("fuuid", "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        SimpleTriggerImpl newTrigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity("fuuid", "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(newTrigger, false);
    }

    @Test
    public void shouldDeleteExistingTrigger() throws JobPersistenceException {
        final String triggerName = id("fuuid");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        couchdbStore.removeTrigger(TriggerKey.triggerKey(triggerName, "borgroup"));
        assertNull(couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName, "borgroup")));
    }

    @Test
    public void shouldDeleteExistingTriggers() throws JobPersistenceException {
        final String triggerName = id("fuuid1");
        SimpleTriggerImpl trigger1 = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        final String triggerName2 = id("fuuid2");
        SimpleTriggerImpl trigger2 = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName2, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger1, false);
        couchdbStore.storeTrigger(trigger2, false);

        couchdbStore.removeTriggers(asList(
            TriggerKey.triggerKey(triggerName, "borgroup"),
            TriggerKey.triggerKey(triggerName2, "borgroup")
        ));

        assertNull(couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName, "borgroup")));
        assertNull(couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName2, "borgroup")));
    }

    @Test
    public void shouldCheckWhetherTriggerExists() throws JobPersistenceException {
        final String triggerName = id("fuuid");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();

        assertFalse(couchdbStore.checkExists(TriggerKey.triggerKey(triggerName, "borgroup")));
        couchdbStore.storeTrigger(trigger, false);
        assertTrue(couchdbStore.checkExists(TriggerKey.triggerKey(triggerName, "borgroup")));
    }

    @Test
    public void shouldFetchTriggersByKeys() throws JobPersistenceException {
        String triggerName = id("fuuid");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        String triggerName2 = id("fuuid2");
        SimpleTriggerImpl trigger2 = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName2, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger2, false);

        String triggerName3 = id("fuuid2");
        SimpleTriggerImpl trigger3 = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName3, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger3, false);

        List<CouchDbTrigger> triggers = couchdbStore.getTriggerStore().getTriggersByKeys(asList(TriggerKey.triggerKey(triggerName, "borgroup"), TriggerKey.triggerKey(triggerName3, "borgroup")));
        List<TriggerKey> triggerKeys = extract(triggers, on(CouchDbTrigger.class).getKey());

        assertEquals(2, triggers.size());
        assertTrue(triggerKeys.contains(trigger.getKey()));
        assertTrue(triggerKeys.contains(trigger3.getKey()));
    }

    @Test
    public void shouldReplaceExistingTrigger() throws JobPersistenceException {
        final String triggerName = id("fuuid");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        SimpleTriggerImpl newTrigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2012, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();

        assertTrue(couchdbStore.replaceTrigger(TriggerKey.triggerKey(triggerName, "borgroup"), newTrigger));
        assertEquals(new Date(2012, 10, 20), couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName, "borgroup")).getStartTime());
    }

    @Test(expected = JobPersistenceException.class)
    public void shouldNotReplaceExistingTriggerIdJobIsDifferent() throws JobPersistenceException {
        final String triggerName = id("fuuid");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid1", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        SimpleTriggerImpl newTrigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid2", "bargroup"))
                .startAt(new Date(2012, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.replaceTrigger(TriggerKey.triggerKey(triggerName, "borgroup"), newTrigger);
    }

    @Test
    public void shouldNotReplaceNonExistingTrigger() throws JobPersistenceException {
        final String triggerName = id("fuuid");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();

        assertFalse(couchdbStore.replaceTrigger(TriggerKey.triggerKey(triggerName, "borgroup"), trigger));
    }

    @Test
    public void shouldRetrieveTriggersByCalendarName() throws JobPersistenceException {
        final String triggerName1 = id("fuuid1");
        int numberOfCalendar = couchdbStore.findTriggersByCalendarName("testCalendar").size();
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName1, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        final String triggerName2 = id("fuuid2");
        SimpleTriggerImpl newTrigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName2, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2012, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .modifiedByCalendar("testCalendar")
                .build();
        couchdbStore.storeTrigger(newTrigger, false);

        List<OperableTrigger> triggers = couchdbStore.findTriggersByCalendarName("testCalendar");
        assertEquals(numberOfCalendar + 1, triggers.size());
        TriggerKey key = null;
        for(Trigger t: triggers) {
            if (t.getKey().getName().equals(triggerName2)) key = t.getKey();
        }
        assertEquals(TriggerKey.triggerKey(triggerName2, "borgroup"), key);

    }

    @Test
    public void shouldCountNumberOfTriggers() throws JobPersistenceException {
        final int numberOfTriggers = couchdbStore.getNumberOfTriggers();
        final String triggerName1 = id("fuuid1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName1, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        final String triggerName2 = id("fuuid2");
        SimpleTriggerImpl newTrigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName2, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2012, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(newTrigger, false);

        assertEquals(numberOfTriggers + 2, couchdbStore.getNumberOfTriggers());
    }

    @Test
    public void shouldReturnAllTriggerGroupNames() throws JobPersistenceException {
        final String triggerName1 = id("fuuid1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName1, "borgroup1")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        final String triggerName2 = id("fuuid2");
        SimpleTriggerImpl newTrigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName2, "borgroup2")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2012, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(newTrigger, false);

        final List<String> triggerGroupNames = couchdbStore.getTriggerGroupNames();
        assertTrue(triggerGroupNames.contains("borgroup1"));
        assertTrue(triggerGroupNames.contains("borgroup2"));
    }

    @Test
    public void shouldReturnMatchingTriggerKeys() throws JobPersistenceException {
        final String triggerName1 = id("fuuid1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName1, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        final String triggerName2 = id("fuuid2");
        SimpleTriggerImpl newTrigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName2, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2012, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(newTrigger, false);

        Set<TriggerKey> triggerKeys = couchdbStore.getTriggerKeys(GroupMatcher.<TriggerKey>groupEquals("borgroup"));
        assertTrue(triggerKeys.contains(TriggerKey.triggerKey(triggerName1, "borgroup")));
        assertTrue(triggerKeys.contains(TriggerKey.triggerKey(triggerName2, "borgroup")));
    }

    @Test
    public void shouldReturnTriggerState() throws JobPersistenceException {
        final String triggerName = id("fuuid1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup1")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        couchdbStore.storeTrigger(trigger, false);

        assertEquals(Trigger.TriggerState.NORMAL, couchdbStore.getTriggerState(TriggerKey.triggerKey(triggerName, "borgroup1")));
    }

    @Test
    public void shouldAcquireTriggersToFire() throws JobPersistenceException {
        int numberOfTriggers = couchdbStore.acquireNextTriggers(new Date(2010 - 1900, 10, 21).getTime(), Integer.MAX_VALUE, 0).size();
        final String triggerName1 = id("fuuid1");
        SimpleTriggerImpl trigger1 = (SimpleTriggerImpl) newTrigger().withIdentity(triggerName1, "borgroup1")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010 - 1900, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        trigger1.computeFirstFireTime(null);
        couchdbStore.storeTrigger(trigger1, false);

        final String triggerName2 = id("fuuid2");
        SimpleTriggerImpl trigger2 = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName2, "borgroup2")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010 - 1900, 10, 22))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        trigger2.computeFirstFireTime(null);
        couchdbStore.storeTrigger(trigger2, false);

        List<OperableTrigger> triggers = couchdbStore.acquireNextTriggers(new Date(2010 - 1900, 10, 21).getTime(), Integer.MAX_VALUE, 0);
        assertEquals(numberOfTriggers + 1, triggers.size());
        assertEquals(TriggerKey.triggerKey(triggerName1, "borgroup1"), triggers.get(0).getKey());
    }

    @Test
    public void shouldFireTriggers() throws JobPersistenceException {
        final String triggerName = id("fuuid1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup1")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(new Date(2010 - 1900, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .repeatForever())
                .build();
        trigger.computeFirstFireTime(null);
        couchdbStore.storeTrigger(trigger, false);

        List<TriggerFiredResult> firedResults = couchdbStore.triggersFired(Arrays.<OperableTrigger>asList(trigger));

        assertEquals(1, firedResults.size());
        assertEquals(TriggerKey.triggerKey(triggerName, "borgroup1"), firedResults.get(0).getTriggerFiredBundle().getTrigger().getKey());
    }

    @Test
    public void shouldDeleteTriggerAfterFire() throws JobPersistenceException {
        final String jobName = id("job");
        JobDetail job = newJob(CouchDbStoreCalendarRepeatingJobIT.TestListener.class)
                .withIdentity(jobName, "bargroup")
                .usingJobData("foo", "bar")
                .usingJobData("fuu", "baz")
                .build();
        couchdbStore.storeJob(job, false);

        final String triggerName = id("fuuid1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup1")
                .forJob(JobKey.jobKey(jobName, "bargroup"))
                .startAt(new Date(2010 - 1900, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .withRepeatCount(0))
                .build();
        trigger.computeFirstFireTime(null);
        couchdbStore.storeTrigger(trigger, false);

        trigger.triggered(null);

        couchdbStore.triggeredJobComplete(trigger, job, null);

        assertNull(couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName, "borgroup1")));
    }

    @Test
    public void shouldUpdateTriggerForMisfireNowWithExistingRepeatCount() throws Exception {
        String triggerName = id("fuuid");
        DateTime now = new DateTime();
        DateTime startTime = now.minusMinutes(1);
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(startTime.toDate())
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(1)
                        .withRepeatCount(2)
                        .withMisfireHandlingInstructionNowWithExistingCount())
                .build();
        trigger.computeFirstFireTime(null);
        couchdbStore.storeTrigger(trigger, false);

        List<OperableTrigger> acquiredTriggers = couchdbStore.acquireNextTriggers(now.getMillis(), 1, 0);
        assertEquals(1, acquiredTriggers.size());

        SimpleTrigger acquiredTrigger = (SimpleTrigger) acquiredTriggers.get(0);
        assertTrue(acquiredTrigger.getNextFireTime().getTime() - now.getMillis() < 5000);
        assertEquals(2, acquiredTrigger.getRepeatCount());
    }

    @Test
    public void shouldUpdateTriggerForMisfireNextWithRemainingRepeatCount() throws Exception {
        String triggerName = id("fuuid");
        DateTime now = new DateTime();
        DateTime startTime = now.minusMinutes(1);
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, "borgroup")
                .forJob(JobKey.jobKey("fooid", "bargroup"))
                .startAt(startTime.toDate())
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(1)
                        .withRepeatCount(2)
                        .withMisfireHandlingInstructionNowWithRemainingCount())
                .build();
        trigger.computeFirstFireTime(null);
        couchdbStore.storeTrigger(trigger, false);

        List<OperableTrigger> acquiredTriggers = couchdbStore.acquireNextTriggers(now.getMillis(), 1, 0);
        assertEquals(1, acquiredTriggers.size());

        SimpleTrigger acquiredTrigger = (SimpleTrigger) acquiredTriggers.get(0);
        assertTrue(acquiredTrigger.getNextFireTime().getTime() - now.plusMinutes(1).getMillis() < 5000);
        assertEquals(1, acquiredTrigger.getRepeatCount());
    }

    @Test
    public void shouldGetTriggerState() throws Exception {
        final String triggerName = id("fuuid1");
        final String triggerGroup = id("borgroup1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
                .withIdentity(triggerName, triggerGroup)
                .forJob(JobKey.jobKey(id("fooid"), id("bargroup")))
                .startAt(new Date(2010 - 1900, 10, 20))
                .withSchedule(simpleSchedule()
                        .withIntervalInMinutes(2)
                        .withRepeatCount(0))
                .build();
        trigger.computeFirstFireTime(null);
        couchdbStore.storeTrigger(trigger, false);

        assertEquals(Trigger.TriggerState.NORMAL, couchdbStore.getTriggerState(TriggerKey.triggerKey(triggerName, triggerGroup)));
    }
    
    @Test
    public void shouldUpdateTriggerStateToAcquired() throws JobPersistenceException {
        final String triggerName = id("fuuid1");
        final String triggerGroup = id("borgroup1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
            .withIdentity(triggerName, triggerGroup)
            .forJob(JobKey.jobKey(id("fooid"), id("bargroup")))
            .startAt(new Date(2010 - 1900, 10, 20))
            .withSchedule(simpleSchedule()
                .withIntervalInMinutes(2)
                .withRepeatCount(0))
            .build();
        trigger.computeFirstFireTime(null);
        couchdbStore.storeTrigger(trigger, false);

        CouchDbTrigger storedTrigger = new CouchDbSimpleTrigger((SimpleTriggerImpl) couchdbStore.retrieveTrigger(TriggerKey.triggerKey(triggerName, triggerGroup)));
        assertEquals(CouchDbTriggerState.WAITING, storedTrigger.getState());

        couchdbStore.acquireNextTriggers(new Date().getTime(), 1, 0);

        CouchDbSimpleTrigger acquiredTrigger = (CouchDbSimpleTrigger) couchdbStore.getTriggerStore().getTriggerByKey(TriggerKey.triggerKey(triggerName, triggerGroup));
        assertEquals(CouchDbTriggerState.ACQUIRED, acquiredTrigger.getState());
    }

    @Test
    public void shouldAcquireWaitingTriggersOnly() throws JobPersistenceException {
        String triggerName = id("fuuid1");
        String triggerGroup = id("borgroup1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
            .withIdentity(triggerName, triggerGroup)
            .forJob(JobKey.jobKey(id("fooid"), id("bargroup")))
            .startAt(new Date(2010 - 1900, 10, 20))
            .withSchedule(simpleSchedule()
                .withIntervalInMinutes(2)
                .withRepeatCount(0))
            .build();
        trigger.computeFirstFireTime(null);
        couchdbStore.storeTrigger(trigger, false);

        String triggerName2 = id("fuuid2");
        String triggerGroup2 = id("borgroup2");
         SimpleTriggerImpl trigger2 = (SimpleTriggerImpl) newTrigger()
            .withIdentity(triggerName2, triggerGroup2)
            .forJob(JobKey.jobKey(id("fooid"), id("bargroup")))
            .startAt(new Date(2010 - 1900, 10, 20))
            .withSchedule(simpleSchedule()
                .withIntervalInMinutes(2)
                .withRepeatCount(0))
            .build();
        trigger2.computeFirstFireTime(null);

        CouchDbSimpleTrigger couchdbTrigger2 = new CouchDbSimpleTrigger(trigger2);
        couchdbTrigger2.setState(CouchDbTriggerState.ACQUIRED);
        couchdbStore.getTriggerStore().storeTrigger(couchdbTrigger2, true);

        List<OperableTrigger> acquiredTriggers = couchdbStore.acquireNextTriggers(new Date().getTime(), 2, 0);
        assertEquals(1, acquiredTriggers.size());
        assertEquals(TriggerKey.triggerKey(triggerName, triggerGroup), acquiredTriggers.get(0).getKey());
    }

    @Test
    public void shouldReleaseAcquiredTriggers() throws JobPersistenceException {
        String triggerName = id("fuuid1");
        String triggerGroup = id("borgroup1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
            .withIdentity(triggerName, triggerGroup)
            .forJob(JobKey.jobKey(id("fooid"), id("bargroup")))
            .startAt(new Date(2010 - 1900, 10, 20))
            .withSchedule(simpleSchedule()
                .withIntervalInMinutes(2)
                .withRepeatCount(0))
            .build();
        trigger.computeFirstFireTime(null);
        couchdbStore.getTriggerStore().storeTrigger(new CouchDbSimpleTrigger(trigger, CouchDbTriggerState.ACQUIRED), false);

        couchdbStore.releaseAcquiredTrigger(trigger);

        CouchDbTrigger dbTrigger = couchdbStore.getTriggerStore().getTriggerByKey(TriggerKey.triggerKey(triggerName, triggerGroup));
        assertEquals(CouchDbTriggerState.WAITING, dbTrigger.getState());
    }

    @Test
    public void shouldReleaseAcquiredTriggersAfterFiring() throws JobPersistenceException {
        String triggerName = id("fuuid1");
        String triggerGroup = id("borgroup1");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
            .withIdentity(triggerName, triggerGroup)
            .forJob(JobKey.jobKey(id("fooid"), id("bargroup")))
            .startAt(new Date(2010 - 1900, 10, 20))
            .withSchedule(simpleSchedule()
                .withIntervalInMinutes(2)
                .withRepeatCount(0))
            .build();
        trigger.computeFirstFireTime(null);
        couchdbStore.getTriggerStore().storeTrigger(new CouchDbSimpleTrigger(trigger, CouchDbTriggerState.ACQUIRED), false);

        couchdbStore.releaseAcquiredTrigger(trigger);

        CouchDbTrigger dbTrigger = couchdbStore.getTriggerStore().getTriggerByKey(TriggerKey.triggerKey(triggerName, triggerGroup));
        assertEquals(CouchDbTriggerState.WAITING, dbTrigger.getState());
    }
}
