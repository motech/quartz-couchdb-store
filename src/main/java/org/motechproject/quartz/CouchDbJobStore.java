package org.motechproject.quartz;

import org.apache.log4j.Logger;
import org.ektorp.ComplexKey;
import org.ektorp.CouchDbConnector;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.support.CouchDbRepositorySupport;
import org.ektorp.support.View;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static ch.lambdaj.Lambda.extract;
import static ch.lambdaj.Lambda.on;

public class CouchDbJobStore extends CouchDbRepositorySupport<CouchDbJobDetail> {
    private Logger logger = Logger.getLogger(CouchDbJobStore.class);

    protected CouchDbJobStore(CouchDbConnector db) {
        super(CouchDbJobDetail.class, db);
        initStandardDesignDocument();
    }

    public void storeJob(CouchDbJobDetail newJob, boolean replaceExisting) throws JobPersistenceException {
        CouchDbJobDetail existingJob = retrieveJob(newJob.getKey());
        if (existingJob == null) {
            db.create(newJob);
            return;
        }
        if (replaceExisting) {
            newJob.setId(existingJob.getId());
            newJob.setRevision(existingJob.getRevision());
            db.update(newJob);
        } else {
            throw new ObjectAlreadyExistsException("job already exists");
        }
    }

    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        CouchDbJobDetail job = retrieveJob(jobKey);
        if (job == null) {
            return false;
        }
        db.delete(job);
        return true;
    }

    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        boolean allFound = true;
        for (JobKey key : jobKeys) {
            allFound = removeJob(key) && allFound;
        }
        return allFound;
    }

    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return retrieveJob(jobKey) != null;
    }

    public CouchDbJobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        try {
            return get(CouchDbJobDetail.toId(jobKey.getGroup(), jobKey.getName()));
        } catch (DocumentNotFoundException ex) {
            return null;
        }
    }

    @View(name = "all_jobs", map = "function(doc) { if (doc.type === 'CouchDbJobDetail') emit(doc._id, doc._id); }")
    public List<CouchDbJobDetail> getAll() {
        return db.queryView(createQuery("all_jobs").includeDocs(true), type);
    }

    @View(name = "by_jobkey", map = "function(doc) { if (doc.type === 'CouchDbJobDetail') emit([doc.name, doc.group], doc._id); }")
    public List<CouchDbJobDetail> getJobs(List<JobKey> jobKeys) {
        List<ComplexKey> keys = new ArrayList<ComplexKey>();
        for (JobKey jobKey : jobKeys) {
            keys.add(ComplexKey.of(jobKey.getName(), jobKey.getGroup()));
        }
        return db.queryView(createQuery("by_jobkey").includeDocs(true).keys(keys), type);
    }

    public int getNumberOfJobs() {
        return getAll().size();
    }

    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) {
        Set<JobKey> matchedKeys = new HashSet<JobKey>();
        for (CouchDbJobDetail jobDetail : getAll()) {
            if (matcher.isMatch(jobDetail.getKey())) {
                matchedKeys.add(jobDetail.getKey());
            }
        }
        return matchedKeys;
    }

    @View(name = "by_jobGroupName", map = "function(doc) { if (doc.type === 'CouchDbJobDetail') emit(doc.group, doc._id); }")
    public List<String> getJobGroupNames() {
        return new ArrayList<String>(new HashSet<String>(extract(db.queryView(createQuery("by_jobGroupName").includeDocs(true), type), on(CouchDbJobDetail.class).getGroup())));
    }

    public void removeAll() {
        try {
            for (CouchDbJobDetail doc : getAll()) {
                db.delete(db.get(CouchDbJobDetail.class, doc.getId()));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
