use strict;
use warnings;
use utf8;
use Test::More;
use DBI;
use DBIx::FixtureLoader;
use Test::Requires 'DBD::SQLite';

my $test_db = 'loader.db';
unlink $test_db if -f $test_db;

my $dbh = DBI->connect("dbi:SQLite:./$test_db", '', '', {RaiseError => 1, sqlite_unicode => 1}) or die 'cannot connect to db';
$dbh->do(q{
    CREATE TABLE item (
        id   INTEGER PRIMARY KEY,
        name VARCHAR(255)
    );
});

my $m = DBIx::FixtureLoader->new(
    dbh => $dbh,
);
isa_ok $m, 'DBIx::FixtureLoader';
is $m->_driver_name, 'SQLite';
ok !$m->bulk_insert;

$m->load_fixture('t/data/item.csv');

my $result = $dbh->selectrow_arrayref('SELECT COUNT(*) FROM item');
is $result->[0], 2;

my $rows = $dbh->selectall_arrayref('SELECT * FROM item;', {Slice => {}});
is scalar @$rows, 2;
is $rows->[0]{name}, 'エクスカリバー';

done_testing;

END {
    unlink $test_db if -f $test_db;
}
