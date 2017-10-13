use utf8;

package Icecat::Schema::Result::MeasureSign;

=head1 NAME

Icecat::Schema::Result::MeasureSign

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );

=head1 TABLE

measure_sign

=cut

__PACKAGE__->table("measure_sign");
__PACKAGE__->add_columns(
    measure_sign_id => { data_type => "integer" },
    measure_id      => { data_type => "integer" },
    langid          => { data_type => "integer" },
    value           => {
        data_type => "varchar",
        size      => 255
    },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
    last_published => {
        data_type     => "datetime",
        set_on_create => 1,
    },
);
__PACKAGE__->set_primary_key("measure_sign_id");
__PACKAGE__->add_unique_constraint( [ "measure_id", "langid" ] );

__PACKAGE__->belongs_to(
    language => "Icecat::Schema::Result::Language",
    "langid"
);

__PACKAGE__->belongs_to(
    measure => "Icecat::Schema::Result::Measure",
    "measure_id"
);

1;
