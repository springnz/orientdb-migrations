package ylabs.orientdb.functions;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

import java.util.Date;

public class OSQLFunctions {

    public static OSQLFunctionAbstract dateTimeAddSecondsFunction() {
      return new OSQLFunctionAbstract("dateTimePlusSeconds", 2, 2) {
          @Override
          public Object execute(
                  Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult,
                  Object[] iParams, OCommandContext iContext) {

              if (iParams[0] == null || iParams[1] == null) {
                  return null;
              }

              if (!(iParams[0] instanceof Date) || !(iParams[1] instanceof Number)) {
                  return null;
              }

              final Date timestamp = (Date) iParams[0];
              final Number seconds = (Number) iParams[1];
              timestamp.setTime(timestamp.getTime() + seconds.longValue() * 1000);
              return timestamp;
          }

          @Override
          public String getSyntax() {
              return "dateTimePlusSeconds(<datetime>, <seconds>)";
          }

          public boolean aggregateResults() {
              return false;
          }
      };
    }
}
