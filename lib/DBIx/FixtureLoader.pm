package DBIx::FixtureLoader;
use 5.008001;
use strict;
use warnings;

our $VERSION = "0.01";

use File::Basename qw/basename/;
use SQL::Maker;
use Carp qw/croak/;

use Moo;

has dbh => (
    is       => 'ro',
    isa      => sub { shift->isa('DBI::db') },
    required => 1,
);

has bulk_insert => (
    is      => 'lazy',
    default => sub {
        my $self = shift;
        my $driver_name = $self->_driver_name;
        my $dbh         = $self->dbh;
        $driver_name eq 'mysql'                                      ? 1 :
        $driver_name eq 'Pg' && $dbh->{ pg_server_version } >= 82000 ? 1 :
                                                                       0 ;
    },
);

has update => (
    is      => 'ro',
    default => sub { undef },
);

has csv_option => (
    is      => 'ro',
    isa     => sub { ref $_[0] eq 'HASH' },
    default => sub { {} },
);

has _driver_name => (
    is      => 'lazy',
    default => sub {
        shift->dbh->{Driver}{Name};
    },
);

has _sql_builder => (
    is      => 'lazy',
    default => sub {
        DBIx::FixtureLoader::QueryBuilder->new(
            driver => shift->_driver_name,
        );
    }
);

no Moo;

sub load_fixture {
    my $self = shift;
    my $file = shift;
    my %opts = ref $_[0] ? %{$_[0]} : @_;

    if (ref($file) =~ /^(?:ARRAY|HASH)$/) {
        return $self->_load_fixture_from_data(data => $file, %opts);
    }

    my $table = $opts{table};
    unless ($table) {
        my $basename = basename($file);
        ($table) = $basename =~ /^([_A-Za-z0-9]+)/;
    }

    my $format = lc($opts{format} || '');
    unless ($format) {
        ($format) = $file =~ /\.([^.]*$)/;
    }

    my $rows;
    if ($format eq 'csv') {
        $rows = $self->_get_data_from_csv($file);
    }
    else {
        if ($format eq 'json') {
            require JSON;
            my $content = do {
                local $/;
                open my $fh, '<', $file or die $!;
                <$fh>;
            };
            $rows = JSON::decode_json($content);
        }
        elsif ($format =~ /ya?ml/) {
            require YAML::Tiny;
            $rows = YAML::Tiny->read($file) or croak( YAML::Tiny->errstr );
            $rows = $rows->[0];
        }
    }

    $self->load_fixture($rows,
        table  => $table,
        update => $opts{update},
    );
}

sub _get_data_from_csv {
    my ($self, $file) = @_;
    require Text::CSV;
    my $csv = Text::CSV->new({
        binary         => 1,
        blank_is_undef => 1,
        %{ $self->csv_option },
    }) or croak( Text::CSV->error_diag );

    open my $fh, '<', $file or die "$!";
    my $columns = $csv->getline($fh);
    my @records;
    while ( my $row = $csv->getline($fh) ){
        my %cols = map { $columns->[$_] => $row->[$_] } 0..$#$columns;
        push @records, \%cols;
    }
    \@records;
}

sub _load_fixture_from_data {
    my ($self, %args) = @_;
    my ($table, $data) = @args{qw/table data/};

    $data = $self->_normalize_data($data);
    my $update = defined $args{update} ? $args{update} : $self->update;

    if ($update && $self->_driver_name ne 'mysql') {
        croak '`update` option only supprt mysql'
    }

    my $dbh = $self->dbh;
    # needs limit ?
    $dbh->begin_work or croak $dbh->errstr;
    if ($self->bulk_insert) {
        my $opt;
        if ($self->update) {
            $opt->{update} = _build_on_duplicate(keys %{$data->[0]});
        }
        my ($sql, @binds) = $self->_sql_builder->insert_multi( $table, $data, $opt );

        $dbh->do( $sql, undef, @binds ) or croak $dbh->errstr;
    }
    else {
        my $method = $update ? 'insert_on_duplicate' : 'insert';
        for my $row (@$data) {
            my $opt;
            $opt = _build_on_duplicate(keys %$row);
            my ($sql, @binds) = $self->_sql_builder->$method($table, $row, $opt);

            $dbh->do( $sql, undef, @binds ) or croak $dbh->errstr;
        }
    }
    $dbh->commit or croak $dbh->errstr;
}

sub _build_on_duplicate {
    +{ map {($_ => \"VALUES(`$_`)")} @_ };
}

sub _normalize_data {
    my ($self, $data) = @_;
    my @ret;
    if (ref $data eq 'HASH') {
        push @ret, $data->{$_} for keys %$data;
    }
    elsif (ref $data eq 'ARRAY') {
        if ($data->[0] && $data->[0]{data} && ref $data->[0]{data} eq 'HASH') {
            @ret = map { $_->{data} } @$data;
        }
        else {
            @ret = @$data;
        }
    }
    \@ret;
}

package DBIx::FixtureLoader::QueryBuilder;
use parent 'SQL::Maker';
__PACKAGE__->load_plugin('InsertMulti');
__PACKAGE__->load_plugin('InsertOnDuplicate');

1;
__END__

=encoding utf-8

=head1 NAME

DBIx::FixtureLoader - Loading fixtures and inserting to your database

=head1 SYNOPSIS

    use DBI;
    use DBIx::FixtureLoader;
    
    my $dbh = DBI->connect(...);
    my $loader = DBIx::FixtureLoader->new(dbh => $dbh);
    $loader->load_fixture('item.csv');

=head1 DESCRIPTION

DBIx::FixtureLoader is to load fixture data and insert to your database.

=head1 INTEFACE

=head2 Constructor

    $loader = DBIx::FixtureLoader->new(%option)

C<new> is Constructor method. Various options may be set in C<%option>, which affect
the behaviour of the object (Type and defaults in parentheses):

=head3 C<< dbh (DBI::db) >>

Required. Database handler.

=head3 C<< bulk_insert (Bool) >>

Using bulk_insert or not. Default value is depend on your database.

=head3 C<< update (Bool, Default: false) >>

Using C<< INSERT ON DUPLICATE >> or not. It can be used only works on C<mysql>.

=head3 C<< csv_option (HashRef, Default: +{}) >>

Specifying L<Text::CSV>'s option. C<binary> and C<blank_is_undef>
are automatically set.

=head2 Methods

=head3 C<< $loader->load_fixture($file_or_data:(Str|HashRef|ArrayRef), [%option]) >>

Loading fixture and inserting to your database. Table name and file format is guessed from
file name. For example, "item.csv" contains data of "item" table and format is "CSV".

In most cases C<%option> is not needed. Available keys of C<%option> are as follows.

=over

=item C<table:Str>

table name of database.

=item C<format:Str>

data format. "CSV", "YAML" and "JSON" are available.

=item C<update:Bool>

Using C<< ON DUPLICATE KEY UPDATE >> or not. Default value depends on object setting.

=back

=head2 File Name and Data Format

=head3 file name

Data format is guessed from extension. Table name is guessed from basename. Leading alphabets,
underscores and numbers are considered table name. So, C<"user_item-2.csv"> is considered CSV format
and containing data of "user_item" table.

=head3 data format

"CSV", "YAML" and "JSON" are parsable. CSV file must have header line for determining column names.

Datas in "YAML" or "JSON" must be ArrayRef or HashRef containing HashRefs. Each HashRef is the data
of database record and keys of HashRef is matching to column names of the table.

=head1 LICENSE

Copyright (C) Masayuki Matsuki.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Masayuki Matsuki E<lt>y.songmu@gmail.comE<gt>

=cut

