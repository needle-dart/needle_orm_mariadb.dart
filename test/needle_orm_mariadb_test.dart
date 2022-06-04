import 'package:logging/logging.dart';
import 'package:mysql1/mysql1.dart';
import 'package:needle_orm_mariadb/needle_orm_mariadb.dart';
import 'package:test/test.dart';

void main() {
  Logger.root.level = Level.FINE;
  Logger.root.onRecord.listen((record) {
    print(
        '${record.level.name}: ${record.time} ${record.loggerName}: ${record.message}');
  });

  test('test IN', () async {
    var settings = ConnectionSettings(
        host: 'localhost',
        port: 3306,
        user: 'needle',
        password: 'needle',
        db: 'needle');
    var conn = await MySqlConnection.connect(settings);
    var ds = MariaDbDataSource(conn);

    var list =
        await ds.execute('books', "select * from books where id in @idList", {
      'idList': [1, 16]
    });
    list.forEach((book) {
      print(book);
    });
  });
}
