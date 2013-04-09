package org.motechproject.quartz;

import org.apache.log4j.Logger;
import org.ektorp.CouchDbConnector;
import org.ektorp.ViewResult;
import org.ektorp.support.CouchDbRepositorySupport;
import org.ektorp.support.View;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CouchDbCalendarStore extends CouchDbRepositorySupport {

    private Logger logger = Logger.getLogger(CouchDbCalendarStore.class);

    protected CouchDbCalendarStore(CouchDbConnector db) {
        super(CouchDbCalendar.class, db);
        initStandardDesignDocument();
    }

    public void storeCalendar(CouchDbCalendar couchdbCalendar, boolean replaceExisting) throws JobPersistenceException {
        try {
            update(couchdbCalendar, replaceExisting);
        } catch (ObjectAlreadyExistsException e) {
            throw e;
        } catch (Exception e) {
            throw new JobPersistenceException(e.getMessage(), e);
        }
    }

    private void update(CouchDbCalendar couchdbCalendar, boolean replaceExisting) throws JobPersistenceException, IOException, ClassNotFoundException {
        CouchDbCalendar dbCalendar = get(couchdbCalendar.getName());
        if (dbCalendar == null) {
            add(couchdbCalendar);
        } else {
            if (!replaceExisting) {
                throw new ObjectAlreadyExistsException("Calendar " + couchdbCalendar.getName() + " already exists.");
            }
            dbCalendar.setCalendarAsStream(couchdbCalendar.getCalendarAsStream());
            update(dbCalendar);
        }
    }

    public boolean removeCalendar(String calName) throws JobPersistenceException {
        CouchDbCalendar calendar = get(calName);
        if (calendar == null) {
            return false;
        }
        remove(calendar);
        return true;
    }

    @View(name = "by_calendarName", map = "function(doc) { if (doc.type === 'CouchDbCalendar') emit(doc.name, doc._id); }")
    public CouchDbCalendar get(String calName) {
        List<CouchDbCalendar> calendars = db.queryView(createQuery("by_calendarName").key(calName).includeDocs(true), type);
        return calendars != null && calendars.size() > 0 ? calendars.get(0) : null;
    }

    public List<CouchDbCalendar> getCalendars(List<String> calendarNames) {
        return db.queryView(createQuery("by_calendarName").includeDocs(true).keys(calendarNames), type);
    }

    @View(name = "all_calendars", map = "function(doc) { if (doc.type === 'CouchDbCalendar') emit(doc._id, doc._id); }")
    public List<CouchDbCalendar> getAll() {
        return db.queryView(createQuery("all_calendars").includeDocs(true), type);
    }

    public int getNumberOfCalendars() {
        return getAll().size();
    }

    public List<String> getCalendarNames() {
        final ViewResult result = db.queryView(createQuery("by_calendarName"));
        List<String> calendarNames = new ArrayList<String>();
        for (ViewResult.Row row : result.getRows()) {
            calendarNames.add(row.getKey());
        }
        return calendarNames;
    }

    public void removeAll() {
        try {
            for (CouchDbCalendar doc : getAll()) {
                db.delete(db.get(CouchDbCalendar.class, doc.getId()));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
