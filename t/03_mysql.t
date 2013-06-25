use strict;
use warnings;
use utf8;
use Test::More;
use DBI;
use DBIx::FixtureLoader;
use Test::Requires 'Test::mysqld';

my $mysqld = Test::mysqld->new(my_cnf => {'skip-networking' => ''}) or plan skip_all => $Test::mysqld::errstr;
my $dbh = DBI->connect($mysqld->dsn, '', '', {RaiseError => 1, mysql_enable_utf8 => 1}) or die 'cannot connect to db';


for my $cond ([], [bulk_insert => 0]) {
    my @cond = @$cond;
    my $bulk = @cond ? 'no bulk' : 'bulk insert';
    note $bulk;

    $dbh->do(q{DROP TABLE IF EXISTS item;});
    $dbh->do(q{
        CREATE TABLE item (
            id   INTEGER PRIMARY KEY,
            name VARCHAR(255)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    });

    my $m = DBIx::FixtureLoader->new(
        dbh => $dbh,
        @cond,
    );
    isa_ok $m, 'DBIx::FixtureLoader';
    is $m->_driver_name, 'mysql';
    ok  $m->bulk_insert if $bulk eq 'bulk insert';
    ok !$m->bulk_insert if $bulk eq 'no bulk';

    $m->load_fixture('t/data/item.csv');

    my $result = $dbh->selectrow_arrayref('SELECT COUNT(*) FROM item');
    is $result->[0], 2;

    my $rows = $dbh->selectall_arrayref('SELECT * FROM item;', {Slice => {}});
    is scalar @$rows, 2;
    is $rows->[0]{name}, 'エクスカリバー';

    subtest update => sub {
        my $m = DBIx::FixtureLoader->new(
            dbh    => $dbh,
            update => 1,
            @cond,
        );
        $m->load_fixture('t/data/item-update.csv');

        my $rows = $dbh->selectall_arrayref('SELECT * FROM item;', {Slice => {}});
        is scalar @$rows, 2;
        is $rows->[0]{name}, 'エクスカリパー';
    };

    subtest ignore => sub {
        my $m = DBIx::FixtureLoader->new(
            dbh    => $dbh,
            ignore => 1,
            @cond,
        );
        $m->load_fixture('t/data/item.csv', ignore => 1);

        my $rows = $dbh->selectall_arrayref('SELECT * FROM item;', {Slice => {}});
        is scalar @$rows, 2;
        is $rows->[0]{name}, 'エクスカリパー';
    };
}

done_testing;
