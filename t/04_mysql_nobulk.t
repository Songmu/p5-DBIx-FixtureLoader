use strict;
use warnings;
use utf8;
use Test::More;
use DBI;
use DBIx::FixtureManager;
use Test::Requires 'Test::mysqld';

my $mysqld = Test::mysqld->new(my_cnf => {'skip-networking' => ''}) or plan skip_all => $Test::mysqld::errstr;
my $dbh = DBI->connect($mysqld->dsn, '', '', {RaiseError => 1, mysql_enable_utf8 => 1}) or die 'cannot connect to db';
$dbh->do(q{
    CREATE TABLE item (
        id   INTEGER PRIMARY KEY,
        name VARCHAR(255)
    );
});

my $m = DBIx::FixtureManager->new(
    dbh         => $dbh,
    bulk_insert => 0,
);
isa_ok $m, 'DBIx::FixtureManager';
is $m->driver_name, 'mysql';
ok !$m->bulk_insert;

$m->load_fixture('t/data/item.csv');

my $result = $dbh->selectrow_arrayref('SELECT COUNT(*) FROM item');
is $result->[0], 2;

my $rows = $dbh->selectall_arrayref('SELECT * FROM item;', {Slice => {}});
is scalar @$rows, 2;
is $rows->[0]{name}, 'エクスカリバー';

subtest update => sub {
    my $m = DBIx::FixtureManager->new(
        dbh         => $dbh,
        bulk_insert => 0,
        update      => 1,
    );
    $m->load_fixture('t/data/item-update.csv');

    my $rows = $dbh->selectall_arrayref('SELECT * FROM item;', {Slice => {}});
    is scalar @$rows, 2;
    is $rows->[0]{name}, 'エクスカリパー';
};

done_testing;
