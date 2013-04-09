package org.motechproject.quartz;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.ektorp.support.CouchDbDocument;
import org.ektorp.support.TypeDiscriminator;
import org.quartz.Calendar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@TypeDiscriminator("doc.type === 'CouchDbCalendar'")
public class CouchDbCalendar extends CouchDbDocument {

    @JsonProperty
    private String name;
    @JsonIgnore
    private Calendar calendar;

    @JsonProperty
    private String type = "CouchDbCalendar";

    private CouchDbCalendar() {
    }

    public CouchDbCalendar(String name, Calendar calendar) {
        this();
        this.calendar = calendar;
        this.name = name;
    }

    public byte [] getCalendarAsStream() throws IOException {
        return serializeCalendar(calendar);
    }
    public void setCalendarAsStream(byte [] data) throws IOException, ClassNotFoundException {
        this.calendar = deserializeCalendar(data);
    }

    private Calendar deserializeCalendar(byte[] data) throws IOException, ClassNotFoundException {
        InputStream binaryInput = new ByteArrayInputStream(data);
        ObjectInputStream in = new ObjectInputStream(binaryInput);
        try {
            return (Calendar) in.readObject();
        } finally {
            in.close();
        }
    }

    private byte[] serializeCalendar(Calendar calendar) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        if (null != calendar) {
            ObjectOutputStream out = new ObjectOutputStream(baos);
            out.writeObject(calendar);
            out.flush();
        }
        return baos.toByteArray();
    }

    @JsonIgnore
    public Calendar getCalendar() {
        return calendar;
    }

    @JsonIgnore
    public String getName() {
        return name;
    }
}

