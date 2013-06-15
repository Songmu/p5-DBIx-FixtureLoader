requires 'DBI';
requires 'Moo';
requires 'SQL::Maker';
requires 'parent';
requires 'perl', '5.008001';

recommends 'Text::CSV';
recommends 'JSON';
recommends 'YAML::Tiny';

on configure => sub {
    requires 'CPAN::Meta';
    requires 'CPAN::Meta::Prereqs';
    requires 'Module::Build';
};

on test => sub {
    requires 'Test::More', '0.98';
    requires 'Test::Requires';
};
