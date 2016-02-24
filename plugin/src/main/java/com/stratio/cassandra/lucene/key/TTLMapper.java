package com.stratio.cassandra.lucene.key;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class TTLMapper {

    protected static final Logger logger = LoggerFactory.getLogger(TTLMapper.class);

    /** The {@code String} representation of a true value. */
    private static final String TRUE = "true";

    /** The {@code String} representation of a false value. */
    private static final String FALSE = "false";
    /** The Lucene field name */
    public static final String FIELD_NAME = "_ttl";

    /** The Lucene field type */
    static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.freeze();
    }


    private boolean hasAnyExpiringCell(Row row) {
        for (ColumnDefinition columnDefinition : row.columns()) {
            Cell cell = row.getCell(columnDefinition);
            if (cell.isExpiring()) {
                logger.trace("cell {} whit name: {} isExpring == True",cell,columnDefinition.name);
                return true;
            }
        }
        return false;
    }
    /**
     * Adds to the specified {@link Document} the {@link Field}s associated to the token of the specified row key.
     *
     * @param document A {@link Document}.
     */
    public void addFields(Document document,Row row) {
        logger.trace("addFields row: {} doc: {}",row,document);
        if (hasAnyExpiringCell(row)) {
            document.add(new StoredField(FIELD_NAME, TRUE));
        }
    }

    public boolean hasExpiring(Document doc) {
        IndexableField field =doc.getField(FIELD_NAME);
        if (field!=null) {
            String value=field.stringValue();
            if (value!=null) {
                return (value.equals(TRUE));
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
