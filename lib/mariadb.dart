import 'dart:async';
import 'dart:collection';
import 'package:logging/logging.dart';
import 'package:mysql1/mysql1.dart';
import 'package:needle_orm/needle_orm.dart';

class MariaDbDataSource extends Database {
  late Logger logger;

  final MySqlConnection _connection;

  MariaDbDataSource(this._connection, {Logger? logger})
      : super(DatabaseType.MariaDB, '10.0') {
    this.logger = logger ?? Logger('MariaDbDatabase');
  }

  @override
  Future<void> close() {
    return _connection.close();
  }

  @override
  Future<DbQueryResult> query(
      String sql, Map<String, dynamic> substitutionValues,
      {List<String> returningFields = const [], String? tableName}) async {
    var params = _sortedValues(sql, substitutionValues);

    for (var name in substitutionValues.keys) {
      if (substitutionValues[name] is List) {
        // expand List, for example : id IN @idList => id IN (?,?,?)
        var list = substitutionValues[name] as List;
        var q = List.filled(list.length, '?', growable: false).join(',');
        sql = sql.replaceAll('@$name', '($q)');
      } else {
        sql = sql.replaceAll('@$name', '?');
      }
    }

    var params2 = [];
    for (var p in params) {
      if (p is List) {
        // expand params for List
        var list = p as List;
        params2.addAll(list);
      } else {
        params2.add(p);
      }
    }

    if (returningFields.isNotEmpty) {
      sql += ' RETURNING ${returningFields.join(',')}';
    }

    logger.fine('query: $sql');
    logger.fine('params: $params2');
    var results = await _connection.query(sql, params2);
    return MariaDbQueryResult(results);
    // return results.map((r) => r.toList()).toList();
  }

  static List<dynamic> _sortedValues(
      String query, Map<String, dynamic> substitutionValues) {
    List<_Position> positions = [];
    for (var name in substitutionValues.keys) {
      for (var start = 0;
          start < query.length &&
              (start = query.indexOf('@$name', start)) != -1;
          start++) {
        positions.add(_Position(name, start));
      }
    }
    positions.sort((a, b) => a.position.compareTo(b.position));
    return positions.map((p) => substitutionValues[p.name]).toList();
  }

  @override
  Future<T> transaction<T>(FutureOr<T> Function(Database) f) async {
    T? returnValue = await _connection.transaction((ctx) async {
      var conn = ctx as MySqlConnection;
      try {
        logger.fine('Entering transaction');
        var tx = MariaDbDataSource(conn, logger: logger);
        return await f(tx);
      } catch (e) {
        logger.severe('Failed to run transaction', e);
        rethrow;
      } finally {
        logger.fine('Exiting transaction');
      }
    });

    return returnValue!;
  }
}

class _Position {
  final String name;
  final int position;
  _Position(this.name, this.position);
}

class MariaDbQueryResult extends DbQueryResult with ListMixin<List> {
  final Results _result;
  final List<ResultRow> rows;

  MariaDbQueryResult(this._result) : rows = _result.toList();

  @override
  int get length => _result.length;
  void set length(int) {
    throw UnimplementedError();
  }

  @override
  List operator [](int index) {
    return rows[index];
  }

  @override
  void operator []=(int index, List value) {
    throw UnimplementedError();
  }

  @override
  int? get affectedRowCount => _result.affectedRows;

  @override
  List<DbColumnDescription> get columnDescriptions =>
      _result.fields.map((desc) => MariaDbColumnDescription(desc)).toList();
}

class MariaDbColumnDescription extends DbColumnDescription {
  final Field desc;
  MariaDbColumnDescription(this.desc);

  /// The name of the column returned by the query.
  String get columnName => desc.name ?? '';

  /// The resolved name of the referenced table.
  String get tableName => desc.table ?? '';
}
