package DBIx::FixtureManager;
use 5.008001;
use strict;
use warnings;

our $VERSION = "0.01";

use File::Basename qw/basename/;
use SQL::Maker;
use Carp;

use Moo;

has dbh => (
    is       => 'ro',
    isa      => sub { shift->isa('DBI::db') },
    required => 1,
);

has driver_name => (
    is => 'lazy',
    default => sub {
        shift->dbh->{Driver}{Name};
    },
);

has bulk_insert => (
    is => 'lazy',
    default => sub {
        my $self = shift;
        my $driver_name = $self->driver_name;
        my $dbh         = $self->dbh;
        $driver_name eq 'mysql'                                      ? 1 :
        $driver_name eq 'Pg' && $dbh->{ pg_server_version } >= 82000 ? 1 :
                                                                       0 ;
    },
);

has _sql_builder => (
    is => 'lazy',
    default => sub {
        DBIx::FixtureManager::QueryBuilder->new(
            driver => shift->driver_name,
        );
    }
);

no Moo;

# needs on duplicate key update ?
sub load_fixture {
    my $self = shift;

    my %args =
        @_ > 1    ? @_              :
        ref $_[0] ? %{$_[0]}        :
                    (file => $_[0]) ;
    my $file = $args{file};

    my $table = $args{table};
    unless ($table) {
        my $basename = basename($file);
        ($table) = $basename =~ /^([_A-Za-z0-9]+)/;
    }

    my $format = lc($args{format} || '');
    unless ($format) {
        ($format) = $file =~ /\.([^.]*$)/;
    }

    my $rows;
    if ($format eq 'csv') {
        $rows = $self->get_data_from_csv($file);
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
            $rows = YAML::Tiny->read($file)->[0];
        }
    }

    $self->load_fixture_from_data(
        table => $table,
        data  => $rows,
    );
}

sub load_fixture_from_data {
    my ($self, %args) = @_;
    my ($table, $data) = @args{qw/table data/};
    $data = $self->_normalize_data($data);

    my $dbh = $self->dbh;
    # needs limit ?
    $dbh->begin_work or croak $dbh->errstr;
    if ($self->bulk_insert) {
        my ($sql, @binds) = $self->_sql_builder->insert_multi( $table, $data );

        $dbh->do( $sql, undef, @binds ) or croak $dbh->errstr;
    }
    else {
        for my $row (@$data) {
            my ($sql, @binds) = $self->_sql_builder->insert($table, $row);
            $dbh->do( $sql, undef, @binds ) or croak $dbh->errstr;
        }
    }
    $dbh->commit or croak $dbh->errstr;
}

sub get_data_from_csv {
    my ($self, $file) = @_;
    require Text::CSV;
    my $csv = Text::CSV->new({binary => 1});

    open my $fh, '<', $file or die "$!";
    my $columns = $csv->getline($fh);
    my @records;
    while ( my $row = $csv->getline($fh) ){
        my %cols =
            map  { $columns->[$_] => $row->[$_] }
            grep { defined($row->[$_]) && $row->[$_] ne '' } 0..$#$columns;

        push @records, \%cols;
    }
    \@records;
}

sub _normalize_data {
    my ($self, $data) = @_;
    my @ret;
    if (ref $data eq 'HASH') {
        push @ret, $data->{$_} for keys %$data;
    }
    elsif (ref $data eq 'ARRAY') {
        @ret = @$data;
    }
    \@ret;
}

package DBIx::FixtureManager::QueryBuilder;
use parent 'SQL::Maker';
__PACKAGE__->load_plugin('InsertMulti');

1;
__END__

=encoding utf-8

=head1 NAME

DBIx::FixtureManager - It's new $module

=head1 SYNOPSIS

    use DBIx::FixtureManager;

=head1 DESCRIPTION

DBIx::FixtureManager is ...

=head1 LICENSE

Copyright (C) Masayuki Matsuki.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Masayuki Matsuki E<lt>y.songmu@gmail.comE<gt>

=cut

