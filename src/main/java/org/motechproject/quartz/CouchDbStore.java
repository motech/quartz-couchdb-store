package org.motechproject.quartz;

import org.apache.log4j.Logger;
import org.ektorp.CouchDbConnector;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import org.ektorp.spring.HttpClientFactoryBean;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Scheduler;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.jdbcjobstore.Constants;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class CouchDbStore implements JobStore {

    private Logger logger = Logger.getLogger(CouchDbStore.class);
    private String instanceId;
    private String instanceName;

    private CouchDbJobStore jobStore;
    private CouchDbTriggerStore triggerStore;
    private CouchDbCalendarStore calendarStore;
    private boolean schedulerRunning;
    private long misfireThreshold = 60000L;

    public CouchDbStore() {
    }

    CouchDbJobStore getJobStore() {
        return jobStore;
    }

    CouchDbTriggerStore getTriggerStore() {
        return triggerStore;
    }

    CouchDbCalendarStore getCalendarStore() {
        return calendarStore;
    }

    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
    }

    public void setProperties(String propertiesFile) throws IOException, CouchDbJobStoreException {
        Properties properties = new Properties();
        properties.load(ClassLoader.class.getResourceAsStream(propertiesFile));

        HttpClientFactoryBean httpClientFactoryBean = new HttpClientFactoryBean();
        httpClientFactoryBean.setProperties(properties);
        httpClientFactoryBean.setCaching(false);
        try {
            httpClientFactoryBean.afterPropertiesSet();

            CouchDbConnector connector = new StdCouchDbConnector("scheduler", new StdCouchDbInstance(httpClientFactoryBean.getObject()));
            this.jobStore = new CouchDbJobStore(connector);
            this.triggerStore = new CouchDbTriggerStore(connector);
            this.calendarStore = new CouchDbCalendarStore(connector);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new CouchDbJobStoreException(e);
        }
    }


    @Override
    public void schedulerStarted() throws SchedulerException {
        //TODO : recover jobs (refer jdbcstore)
        //TODO : handle misfires (refer jdbcstore)
        schedulerRunning = true;
    }

    @Override
    public void schedulerPaused() {
        schedulerRunning = false;
    }

    @Override
    public void schedulerResumed() {
        schedulerRunning = true;
    }


    @Override
    public void shutdown() {
        throw new NotImplementedException();
    }

    @Override
    public boolean supportsPersistence() {
        return true;
    }

    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        return 0; //not used
    }

    @Override
    public boolean isClustered() {
        return false;
    }

    @Override
    public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws JobPersistenceException {
        if(logger.isInfoEnabled()) {
            logger.info("store:" + newJob + " trigger:" + newTrigger);
        }
        jobStore.storeJob(new CouchDbJobDetail(newJob), false);
        triggerStore.storeTrigger(createCouchDbTrigger(newTrigger), false);
    }

    @Override
    public void storeJob(JobDetail newJob, boolean replaceExisting) throws JobPersistenceException {
        jobStore.storeJob(new CouchDbJobDetail(newJob), replaceExisting);
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, List<Trigger>> triggersAndJobs, boolean replace) throws JobPersistenceException {
        if (!replace) {
            for (Map.Entry<JobDetail, List<Trigger>> e : triggersAndJobs.entrySet()) {
                if (checkExists(e.getKey().getKey())) {
                    throw new ObjectAlreadyExistsException(e.getKey());
                }
                for (Trigger trigger : e.getValue()) {
                    if (checkExists(trigger.getKey())) {
                        throw new ObjectAlreadyExistsException(trigger);
                    }
                }
            }
        }
        for (Map.Entry<JobDetail, List<Trigger>> e : triggersAndJobs.entrySet()) {
            storeJob(e.getKey(), true);
            for (Trigger trigger : e.getValue()) {
                storeTrigger((OperableTrigger) trigger, true);
            }
        }
    }

    @Override
    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        for (OperableTrigger trigger : getTriggersForJob(jobKey)) {
            this.removeTrigger(trigger.getKey());
        }
        return jobStore.removeJob(jobKey);
    }

    @Override
    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        return jobStore.removeJobs(jobKeys);
    }

    @Override
    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return jobStore.retrieveJob(jobKey);
    }

    @Override
    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        triggerStore.storeTrigger(createCouchDbTrigger(newTrigger), replaceExisting);
    }

    @Override
    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        CouchDbTrigger trigger = triggerStore.getTriggerByKey(triggerKey);
        if (triggerStore.removeTrigger(triggerKey)) {
            List<CouchDbTrigger> triggers = triggerStore.findByJob(trigger.getJobKey());
            if (triggers == null || triggers.size() <= 1) {
                jobStore.removeJob(trigger.getJobKey());
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        boolean allFound = true;
        for (TriggerKey key : triggerKeys) {
            allFound = triggerStore.removeTrigger(key) && allFound;
        }
        return allFound;
    }

    @Override
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        return triggerStore.replaceTrigger(triggerKey, createCouchDbTrigger(newTrigger));
    }

    @Override
    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        CouchDbTrigger couchdbTrigger = triggerStore.retrieveTrigger(triggerKey);
        return couchdbTrigger != null ? couchdbTrigger.getTrigger() : null;
    }

    @Override
    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return jobStore.checkExists(jobKey);
    }

    @Override
    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        return triggerStore.checkExists(triggerKey);
    }

    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {
        jobStore.removeAll();
        triggerStore.removeAll();
        calendarStore.removeAll();
        if (jobStore.getAll().size() > 0) {
            throw new JobPersistenceException("jobs not cleared");
        }
        if (triggerStore.getAll().size() > 0) {
            throw new JobPersistenceException("triggers not cleared");
        }
        if (calendarStore.getAll().size() > 0) {
            throw new JobPersistenceException("calendars not cleared");
        }
    }

    @Override
    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers) throws JobPersistenceException {
        CouchDbCalendar couchdbCalendar = new CouchDbCalendar(name, calendar);
        calendarStore.storeCalendar(couchdbCalendar, replaceExisting);
        if (updateTriggers) {
            for (CouchDbTrigger trigger : triggerStore.findByCalendarName(name)) {
                trigger.updateWithNewCalendar(getCalendar(name), 1000);
                triggerStore.storeTrigger(trigger, true);
            }
        }
    }

    private Calendar getCalendar(String name) {
        CouchDbCalendar couchdbCalendar = calendarStore.get(name);
        return couchdbCalendar != null ? couchdbCalendar.getCalendar() : null;
    }

    @Override
    public boolean removeCalendar(String calName) throws JobPersistenceException {
        return calendarStore.removeCalendar(calName);
    }

    @Override
    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        return getCalendar(calName);
    }

    @Override
    public int getNumberOfJobs() throws JobPersistenceException {
        return jobStore.getNumberOfJobs();
    }

    @Override
    public int getNumberOfTriggers() throws JobPersistenceException {
        return triggerStore.getNumberOfTriggers();
    }

    @Override
    public int getNumberOfCalendars() throws JobPersistenceException {
        return calendarStore.getNumberOfCalendars();
    }

    @Override
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        return jobStore.getJobKeys(matcher);
    }

    @Override
    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return triggerStore.getTriggerKeys(matcher);
    }

    @Override
    public List<String> getJobGroupNames() throws JobPersistenceException {
        return jobStore.getJobGroupNames();
    }

    @Override
    public List<String> getTriggerGroupNames() throws JobPersistenceException {
        return triggerStore.getTriggerGroupNames();
    }

    @Override
    public List<String> getCalendarNames() throws JobPersistenceException {
        return calendarStore.getCalendarNames();
    }

    @Override
    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        List<CouchDbTrigger> couchdbTriggers = triggerStore.findByJob(jobKey);
        List<OperableTrigger> operableTriggers = new ArrayList<OperableTrigger>();
        for (CouchDbTrigger trigger : couchdbTriggers) {
            operableTriggers.add(trigger.getTrigger());
        }
        return operableTriggers;
    }

    @Override
    public Trigger.TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        return triggerStore.getTriggerState(triggerKey);
    }

    @Override
    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        triggerStore.updateTriggerState(triggerKey, Constants.STATE_WAITING);
    }

    @Override
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        throw new NotImplementedException();
    }

    @Override
    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        throw new NotImplementedException();
    }

    @Override
    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        throw new NotImplementedException();
    }

    @Override
    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        throw new NotImplementedException();
    }

    @Override
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        throw new NotImplementedException();
    }

    @Override
    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        throw new NotImplementedException();
    }

    @Override
    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        throw new NotImplementedException();
    }

    @Override
    public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        throw new NotImplementedException();
    }

    @Override
    public void pauseAll() throws JobPersistenceException {
        throw new NotImplementedException();
    }

    @Override
    public void resumeAll() throws JobPersistenceException {
        throw new NotImplementedException();
    }

    protected boolean applyMisfire(OperableTrigger trigger) throws JobPersistenceException {

        long misfireTime = System.currentTimeMillis();
        if (getMisfireThreshold() > 0) {
            misfireTime -= getMisfireThreshold();
        }

        Date tnft = trigger.getNextFireTime();
        if (tnft == null || tnft.getTime() > misfireTime
            || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
            return false;
        }

        Calendar cal = null;
        if (trigger.getCalendarName() != null) {
            cal = retrieveCalendar(trigger.getCalendarName());
        }

        trigger.updateAfterMisfire(cal);

        if (trigger.getNextFireTime() == null) {
        } else if (tnft.equals(trigger.getNextFireTime())) {
            return false;
        }
        return true;
    }

    private long getMisfireThreshold() {
        return this.misfireThreshold;
    }

    public void setMisfireThreshold(long misfireThreshold) {
        if (misfireThreshold < 1) {
            throw new IllegalArgumentException("Misfirethreshold must be larger than 0");
        }
        this.misfireThreshold = misfireThreshold;
    }

    @Override
    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("acquireNextTriggers: [%s], maxCount [%s], timeWindow [%s]", noLaterThan, maxCount, timeWindow));
        }
        List<CouchDbTrigger> couchdbTriggers = triggerStore.acquireNextTriggers(noLaterThan, maxCount, timeWindow);

        List<OperableTrigger> operableTriggers = new ArrayList<OperableTrigger>();
        for (CouchDbTrigger couchdbTrigger : couchdbTriggers) {
            applyMisfire(couchdbTrigger.getTrigger());
            couchdbTrigger.setState(CouchDbTriggerState.ACQUIRED);
            operableTriggers.add(couchdbTrigger.getTrigger());
        }
        triggerStore.updateTriggers(couchdbTriggers);
        if (logger.isInfoEnabled()) {
            logger.info(operableTriggers.size() + " triggers acquired.");
            logger.trace(operableTriggers);
        }
        return operableTriggers;
    }

    @Override
    public void releaseAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
        triggerStore.releaseAcquiredTrigger(createCouchDbTrigger(trigger));
    }

    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        if (logger.isInfoEnabled()) {
            logger.info("Triggers fired " + triggers.size());
            logger.trace(triggers);
        }

        List<CouchDbTrigger> couchdbTriggers = fetchCouchDbTriggers(triggers);
        Map<String, Calendar> triggerCalendars = fetchCalendars(triggers);
        Map<JobKey, JobDetail> jobDetailMap = fetchJobDetails(triggers);

        List<TriggerFiredResult> firedResults = new ArrayList<TriggerFiredResult>();
        List<CouchDbTrigger> firedTriggers = triggerStore.triggersFired(couchdbTriggers, triggerCalendars);
        for (CouchDbTrigger firedTrigger : firedTriggers) {

            Date prevFireTime = find(couchdbTriggers, firedTrigger.getKey()).getPreviousFireTime();
            Calendar calendar = triggerCalendars.get(firedTrigger.getCalendarName());
            JobDetail job = jobDetailMap.get(firedTrigger.getJobKey());

            TriggerFiredBundle triggerFiredBundle = buildTriggerFiredBundle(firedTrigger, prevFireTime, calendar, job);
            firedResults.add(new TriggerFiredResult(triggerFiredBundle));
        }
        return firedResults;
    }

    private Map<JobKey, JobDetail> fetchJobDetails(List<OperableTrigger> triggers) {
        Set<JobKey> jobKeys = new HashSet<JobKey>();
        for (OperableTrigger trigger : triggers) {
            jobKeys.add(trigger.getJobKey());
        }
        Map<JobKey, JobDetail> jobDetailMap = new HashMap<JobKey,JobDetail>();
        List<CouchDbJobDetail> jobs = jobStore.getJobs(new ArrayList<JobKey>(jobKeys));
        for (CouchDbJobDetail job : jobs) {
            jobDetailMap.put(job.getKey(), job);
        }
        return jobDetailMap;
    }

    private Map<String, Calendar> fetchCalendars(List<OperableTrigger> triggers) {
        Set<String> calendarNames = new HashSet<String>();
        for (OperableTrigger trigger : triggers) {
            calendarNames.add(trigger.getCalendarName());
        }
        Map<String, Calendar> calendarMap = new HashMap<String, Calendar>();
        List<CouchDbCalendar> calendars = calendarStore.getCalendars(new ArrayList<String>(calendarNames));
        for (CouchDbCalendar calendar : calendars) {
            calendarMap.put(calendar.getName(), calendar.getCalendar());
        }
        return calendarMap;
    }

    private List<CouchDbTrigger> fetchCouchDbTriggers(List<OperableTrigger> triggers) throws JobPersistenceException {
        List<TriggerKey> triggerKeys = new ArrayList<TriggerKey>();
        for (OperableTrigger trigger : triggers) {
                triggerKeys.add(trigger.getKey());
        }
        return triggerStore.getTriggersByKeys(triggerKeys);
    }

    private TriggerFiredBundle buildTriggerFiredBundle(CouchDbTrigger firedTrigger, Date prevFireTime, Calendar calendar, JobDetail job) {
        return new TriggerFiredBundle(
            job,
            firedTrigger.getTrigger(),
            calendar,
            firedTrigger.getKey().getGroup().equals(Scheduler.DEFAULT_RECOVERY_GROUP),
            new Date(),
            firedTrigger.getPreviousFireTime(),
            prevFireTime,
            firedTrigger.getNextFireTime());
    }

    private CouchDbTrigger find(List<CouchDbTrigger> couchdbTriggers, TriggerKey key) {
        for (CouchDbTrigger trigger : couchdbTriggers) {
            if (trigger.getKey().equals(key)) {
                return trigger;
            }
        }
        return null;
    }

    @Override
    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, Trigger.CompletedExecutionInstruction triggerInstCode) throws JobPersistenceException {
        if (trigger.getNextFireTime() == null) {
            triggerStore.removeTrigger(trigger.getKey());
        }
    }

    @Override
    public void setInstanceId(String schedInstId) {
        this.instanceId = schedInstId;
    }

    @Override
    public void setInstanceName(String schedName) {
        this.instanceName = schedName;
    }

    @Override
    public void setThreadPoolSize(int poolSize) {
        // other job stores don't do anything with this
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public List<OperableTrigger> findTriggersByCalendarName(String calName) {
        List<CouchDbTrigger> couchdbTriggers = triggerStore.findByCalendarName(calName);
        List<OperableTrigger> operableTriggers = new ArrayList<OperableTrigger>();
        for (CouchDbTrigger couchdbTrigger : couchdbTriggers) {
            operableTriggers.add(couchdbTrigger.getTrigger());
        }
        return operableTriggers;
    }

    public boolean isSchedulerRunning() {
        return schedulerRunning;
    }

    private CouchDbTrigger createCouchDbTrigger(OperableTrigger newTrigger) {
        CouchDbTrigger couchdbTrigger = null;
        if (newTrigger instanceof SimpleTriggerImpl) {
            couchdbTrigger = new CouchDbSimpleTrigger((SimpleTriggerImpl) newTrigger);
        } else if (newTrigger instanceof CronTriggerImpl) {
            couchdbTrigger = new CouchDbCronTrigger((CronTriggerImpl) newTrigger);
        } else if (newTrigger instanceof CalendarIntervalTriggerImpl) {
            couchdbTrigger = new CouchDbCalendarIntervalTrigger((CalendarIntervalTriggerImpl) newTrigger);
        }
        return couchdbTrigger;
    }

    public static class NotImplementedException extends RuntimeException {
    }
}
