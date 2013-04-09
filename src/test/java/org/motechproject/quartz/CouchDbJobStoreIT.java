package org.motechproject.quartz;

import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.SimpleTriggerImpl;

import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static ch.lambdaj.Lambda.extract;
import static ch.lambdaj.Lambda.on;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.motechproject.quartz.IdRandomizer.id;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class CouchDbJobStoreIT {

    private CouchDbStore couchdbStore;

    @Before
    public void setup() throws Exception, CouchDbJobStoreException {
        couchdbStore = new CouchDbStore();
        couchdbStore.setProperties("/couchdb.properties");
        couchdbStore.clearAllSchedulingData();
    }

    @Test
    public void shouldStoreAndRetrieveJob() throws JobPersistenceException {
        final String jobId = id("fooid");
        JobDetail job = newJob(DummyJobListener.class)
            .withIdentity(jobId, "bargroup")
            .usingJobData("foo", "bar")
            .usingJobData("fuu", "baz")
            .build();
        couchdbStore.storeJob(job, false);

        assertNull(couchdbStore.retrieveJob(JobKey.jobKey("something", "something")));
        assertEquals("bar", couchdbStore.retrieveJob(JobKey.jobKey(jobId, "bargroup")).getJobDataMap().get("foo"));
    }

    @Test
    public void shouldUpdateExistingJob() throws JobPersistenceException {
        final String jobId = id("fooid");
        JobDetail job = newJob(DummyJobListener.class)
            .withIdentity(jobId, "bargroup")
            .usingJobData("foo", "bar")
            .build();
        couchdbStore.storeJob(job, false);

        JobDetail newJob = newJob(DummyJobListener.class)
            .withIdentity(jobId, "bargroup")
            .usingJobData("fii", "bur")
            .build();
        couchdbStore.storeJob(newJob, true);

        assertEquals("bur", couchdbStore.retrieveJob(JobKey.jobKey(jobId, "bargroup")).getJobDataMap().get("fii"));
    }

    @Test(expected = ObjectAlreadyExistsException.class)
    public void shouldNotUpdateExistingJob() throws JobPersistenceException {
        JobDetail job = newJob(DummyJobListener.class)
            .withIdentity("fooid", "bargroup")
            .usingJobData("foo", "bar")
            .build();
        couchdbStore.storeJob(job, false);

        JobDetail newJob = newJob(DummyJobListener.class)
            .withIdentity("fooid", "bargroup")
            .usingJobData("fii", "bur")
            .build();
        couchdbStore.storeJob(newJob, false);

        assertNull(couchdbStore.retrieveJob(JobKey.jobKey("fooid", "bargroup")).getJobDataMap().get("fii"));
    }

    @Test
    public void shouldDeleteExistingJob() throws JobPersistenceException {
        final String jobId = id("fooid");
        JobDetail job = newJob(DummyJobListener.class)
            .withIdentity(jobId, "bargroup")
            .usingJobData("foo", "bar")
            .build();
        couchdbStore.storeJob(job, false);

        couchdbStore.removeJob(JobKey.jobKey(jobId, "bargroup"));
        assertNull(couchdbStore.retrieveJob(JobKey.jobKey(jobId, "bargroup")));
    }

    @Test
    public void shouldDeleteAssociatedTriggersWhenDeletingJob() throws JobPersistenceException {
        final String jobId = id("job");
        JobDetail job = newJob(DummyJobListener.class)
            .withIdentity(jobId, "bargroup")
            .build();
        couchdbStore.storeJob(job, false);

        final String tridderName = id("trigger");
        SimpleTriggerImpl trigger = (SimpleTriggerImpl) newTrigger()
            .withIdentity(tridderName, "borgroup")
            .forJob(JobKey.jobKey(jobId, "bargroup"))
            .startAt(new Date(2010, 10, 20))
            .withSchedule(simpleSchedule()
                .withIntervalInMinutes(2)
                .repeatForever())
            .build();
        couchdbStore.storeTrigger(trigger, false);

        couchdbStore.removeJob(JobKey.jobKey(jobId, "bargroup"));
        assertNull(couchdbStore.retrieveTrigger(TriggerKey.triggerKey(tridderName, "borgroup")));
    }

    @Test
    public void shouldDeleteExistingJobs() throws JobPersistenceException {
        final String job1Id = id("fooid1");
        JobDetail job1 = newJob(DummyJobListener.class).withIdentity(job1Id, "bargroup1").build();
        final String job2Id = id("fooid2"); JobDetail job2 = newJob(DummyJobListener.class)
            .withIdentity(job2Id, "bargroup2")
            .build();
        final String job3Id = id("fooid3"); JobDetail job3 = newJob(DummyJobListener.class)
            .withIdentity(job3Id, "bargroup2")
            .build();

        couchdbStore.storeJob(job1, false);
        couchdbStore.storeJob(job2, false);
        couchdbStore.storeJob(job3, false);

        couchdbStore.removeJobs(asList(
            JobKey.jobKey(job1Id, "bargroup1"),
            JobKey.jobKey(job2Id, "bargroup2")
        ));

        assertNull(couchdbStore.retrieveJob(JobKey.jobKey(job1Id, "bargroup1")));
        assertNull(couchdbStore.retrieveJob(JobKey.jobKey(job2Id, "bargroup2")));
        assertNotNull(couchdbStore.retrieveJob(JobKey.jobKey(job3Id, "bargroup2")));
    }

    @Test
    public void shouldCheckWhetherJobExists() throws JobPersistenceException {
        final String jobId = id("fooid");
        JobDetail job = newJob(DummyJobListener.class).withIdentity(jobId, "bargroup").build();

        assertFalse(couchdbStore.checkExists(JobKey.jobKey(jobId, "bargroup")));
        couchdbStore.storeJob(job, false);
        assertTrue(couchdbStore.checkExists(JobKey.jobKey(jobId, "bargroup")));
    }

    @Test
    public void shouldCountAllJobs() throws JobPersistenceException {
        int numberOfJobsBeforeTest = couchdbStore.getNumberOfJobs();
        JobDetail job1 = newJob(DummyJobListener.class).withIdentity(id("fooid1"), "bargroup1").build();
        JobDetail job2 = newJob(DummyJobListener.class).withIdentity(id("fooid2"), "bargroup2").build();
        JobDetail job3 = newJob(DummyJobListener.class).withIdentity(id("fooid3"), "bargroup2").build();

        couchdbStore.storeJob(job1, false);
        couchdbStore.storeJob(job2, false);
        couchdbStore.storeJob(job3, false);

        assertEquals(numberOfJobsBeforeTest + 3, couchdbStore.getNumberOfJobs());
    }

    @Test
    public void shouldReturnMatchingJobKeys() throws JobPersistenceException {
        JobDetail job1 = newJob(DummyJobListener.class).withIdentity(id("fooid1"), "bargroup1").build();
        final String groupId = id("bargroup2");
        final String job1Id = id("fooid2");
        final String job2Id = id("fooid3");
        JobDetail job2 = newJob(DummyJobListener.class).withIdentity(job1Id, groupId).build();
        JobDetail job3 = newJob(DummyJobListener.class).withIdentity(job2Id, groupId).build();
        couchdbStore.storeJob(job1, false);
        couchdbStore.storeJob(job2, false);
        couchdbStore.storeJob(job3, false);

        assertEquals(
            new HashSet<JobKey>(asList(JobKey.jobKey(job1Id, groupId), JobKey.jobKey(job2Id, groupId))),
            couchdbStore.getJobKeys(GroupMatcher.<JobKey>groupEquals(groupId)));
    }

    @Test
    public void shouldReturnAllJobGroupNames() throws JobPersistenceException {
        final String group1Id = id("group1Id");
        final String group2Id = "group2Id";
        JobDetail job1 = newJob(DummyJobListener.class).withIdentity(id("fooid1"), group1Id).build();
        JobDetail job2 = newJob(DummyJobListener.class).withIdentity(id("fooid2"), group2Id).build();
        JobDetail job3 = newJob(DummyJobListener.class).withIdentity(id("fooid3"), group2Id).build();
        couchdbStore.storeJob(job1, false);
        couchdbStore.storeJob(job2, false);
        couchdbStore.storeJob(job3, false);

        List<String> jobGroupNames = couchdbStore.getJobGroupNames();
        assertTrue(jobGroupNames.contains(group1Id));
        assertTrue(jobGroupNames.contains(group2Id));
    }

    @Test
    public void shouldReturnAllJobs() throws JobPersistenceException {
        String group1Id = id("group1Id");
        String group2Id = id("group2Id");
        String jobid1 = id("fooid1");
        String jobid2 = id("fooid2");
        String jobid3 = id("fooid3");
        JobDetail job1 = newJob(DummyJobListener.class).withIdentity(jobid1, group1Id).build();
        JobDetail job2 = newJob(DummyJobListener.class).withIdentity(jobid2, group2Id).build();
        JobDetail job3 = newJob(DummyJobListener.class).withIdentity(jobid3, group2Id).build();
        couchdbStore.storeJob(job1, false);
        couchdbStore.storeJob(job2, false);
        couchdbStore.storeJob(job3, false);

        List<CouchDbJobDetail> jobs = couchdbStore.getJobStore().getJobs(asList(
            JobKey.jobKey(jobid1, group1Id),
            JobKey.jobKey(jobid3, group2Id)
        ));
        List<JobKey> jobKeys = extract(jobs, on(CouchDbJobDetail.class).getKey());

        assertEquals(2, jobs.size());
        assertTrue(jobKeys.contains(JobKey.jobKey(jobid1, group1Id)));
        assertTrue(jobKeys.contains(JobKey.jobKey(jobid3, group2Id)));
    }
}
