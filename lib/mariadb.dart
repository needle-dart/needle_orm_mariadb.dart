import 'dart:async';
import 'package:logging/logging.dart';
import 'package:mysql1/mysql1.dart';
import 'package:needle_orm/needle_orm.dart';

class MariaDbDataSource extends DataSource {
  late Logger logger;

  final MySqlConnection _connection;

  MariaDbDataSource(this._connection, {Logger? logger})
      : super(DatabaseType.MariaDB, '10.0') {
    this.logger = logger ?? Logger('MariaDbDataSource');
  }

  Future<void> close() {
    return _connection.close();
  }

  @override
  Future<List<List>> execute(
      String tableName, String sql, Map<String, dynamic> substitutionValues,
      [List<String> returningFields = const []]) async {
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

    logger.fine('query: $sql');
    logger.fine('params: $params2');
    var results = await _connection.query(sql, params2);
    if (results.insertId != null) {
      logger.fine('=== inserted with id: ${results.insertId}');
      return [
        [results.insertId]
      ];
    }
    return results.map((r) => r.toList()).toList();
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
  Future<T> transaction<T>(FutureOr<T> Function(DataSource) f) async {
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

/* 
void main(List<String> args) {
  var values = MariaDbDataSource._sortedValues(
      'select * from user where email=@email and name=@name and email2=@email and age=@age and user_name=@name',
      {'name': "ABC", 'age': 18, 'email': 'abc@abc.com'});
  print(values);
}
 */
