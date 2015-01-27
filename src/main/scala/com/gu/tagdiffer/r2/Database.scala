package com.gu.tagdiffer.r2

import java.sql.{Connection, ResultSet}
import java.util.Date
import javax.sql.DataSource
import org.joda.time.{DateTime, LocalDate}

import scalaz.Monoid

/**
 * Represents a potential connection to a database
 */
final class Database(dataSource: DataSource) {

  def apply[X](block: OpenConnection => X): X = {
    val openConnection = new OpenConnection(dataSource.getConnection)
    try {
      block(openConnection)
    } finally {
      openConnection.close
    }
  }

  /**
   * Represents an open connection to a database
   */
  final class OpenConnection(conn: Connection) {
    // scala streams lazy evaluate: so the rowTranslator is only executed when you
    // ask for the next item from the stream
    private def mkStream[A](rs: ResultSet, rowTranslator: Row => A, cleanup: => Unit): Stream[A] = {
      if (rs.next) {
        Stream.cons(rowTranslator(new Row(rs)), mkStream(rs, rowTranslator, cleanup))
      } else {
        rs.close
        cleanup
        Stream.empty
      }
    }

    // perform a query, applying a rowTranslator to each row
    // note this returns a stream, which lazily retrieves stuff from the resultset:
    //  you must finish processing this stream before you close the open database connection
    //  (by exiting the db { } block)
    def query[X](q: Query, rowTranslator: Row => X): Stream[X] = {
      //logTime(q) {
        val statement = q.mkStatement(conn)
        mkStream(statement.executeQuery(), rowTranslator, statement.close)
      //}
    }

    def queryUnique[X](q: Query, rowTranslator: Row => X): X =
      query(q, rowTranslator).head

    def queryUniqueNullable[X](q: Query, rowTranslator: Row => X): Option[X] =
      query(q, rowTranslator).headOption

    def close = conn.close

    /** Applies the function f to each row and accumulates the results in the monoid for A.
      *
      * A monoid for A is an interface which has:
      *   - An "empty" or "zero" element of type A
      *   - An "append" operation, (A, A) => A
      *
      * To accumulate query results, we start with the empty element, then with each subsequent row,
      * transform it to a value of A which we append to the accumulated total.
      *
      * This is particularly useful for accumulating Maps: If type V is a monoid, then Map[K, V] is a monoid
      * where the append operation accumulates values (instead of discarding) upon a key collision.
      */
    def queryFoldMap[A](q: Query)(f: Row => A)(implicit M: Monoid[A]): A =
      query(q, f).foldLeft(M.zero)(M.append(_, _))

  }

}

final class Row(val rs: ResultSet) {
  def apply(columnName: String) = new ColumnValue(rs, columnName)
  def field(columnName: String) = apply(columnName)
}

final class ColumnValue(rs: ResultSet, columnName: String) {
  def nullableString: Option[String] = { val s = rs.getString(columnName); if (rs.wasNull) None else Some(s) }
  def string: String = nullableString getOrElse reportAttemptToGetNullColumn

  def nullableInt: Option[Int] = { val i = rs.getInt(columnName); if (rs.wasNull) None else Some(i) }
  def int: Int = nullableInt getOrElse reportAttemptToGetNullColumn

  def nullableLong: Option[Long] = {val l = rs.getLong(columnName); if (rs.wasNull) None else Some(l)}
  def long: Long = nullableLong getOrElse reportAttemptToGetNullColumn

  def nullableDateTime: Option[Date] = { val d = rs.getTimestamp(columnName); if (rs.wasNull) None else Some(d) }
  def dateTime: Date = nullableDateTime getOrElse reportAttemptToGetNullColumn

  def nullableBoolean: Option[Boolean] = { val b = rs.getBoolean(columnName); if (rs.wasNull) None else Some(b) }
  def boolean: Boolean = nullableBoolean getOrElse reportAttemptToGetNullColumn

  def nullableDouble: Option[Double] = { val d = rs.getDouble(columnName); if (rs.wasNull) None else Some(d) }
  def double: Double = nullableDouble getOrElse reportAttemptToGetNullColumn

  def nullableJodaLocalDate: Option[LocalDate] = {
    val date = { val d = rs.getDate(columnName); if (rs.wasNull) None else Some(d) }
    date map { d => new LocalDate(d) }
  }

  def nullableJodaDateTime: Option[DateTime] = {
    val date = { val d = rs.getTimestamp(columnName); if (rs.wasNull) None else Some(d) }
    date map { d => new DateTime(d) }
  }

  def jodaDateTime: DateTime = { new DateTime(dateTime) }

  private def reportAttemptToGetNullColumn: Nothing =
    sys.error("Attempt to get value of null column "+ columnName + "; assign to an Option instead")
}
