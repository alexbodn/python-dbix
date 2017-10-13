use utf8;

package Icecat::Schema::Result::Measure;

=head1 NAME

Icecat::Schema::Result::Measure

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );

=head1 TABLE

measure

=cut

__PACKAGE__->table("measure");
__PACKAGE__->add_columns(
    measure_id => { data_type => "integer" },
    sid        => { data_type => "integer" },
    tid        => { data_type => "integer" },
    sign       => { data_type => "varchar", is_nullable => 1, size => 255 },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
    last_published        => { data_type => "datetime", set_on_create => 1, },
    system_of_measurement => {
        data_type     => "enum",
        default_value => "metric",
        extra         => { list => [ "metric", "imperial" ] },
    },
);
__PACKAGE__->set_primary_key("measure_id");

__PACKAGE__->has_many(
    descriptions => "Icecat::Schema::Result::Tex",
    { 'foreign.tid' => 'self.tid' }
);

__PACKAGE__->has_many(
    features => "Icecat::Schema::Result::Feature",
    "measure_id"
);

__PACKAGE__->has_many(
    names => "Icecat::Schema::Result::Vocabulary",
    { 'foreign.sid' => 'self.sid' }
);

__PACKAGE__->belongs_to(
    sidindex => "Icecat::Schema::Result::SidIndex",
    "sid"
);

__PACKAGE__->has_many(
    signs => "Icecat::Schema::Result::MeasureSign",
    "measure_id"
);

__PACKAGE__->belongs_to(
    tidindex => "Icecat::Schema::Result::TidIndex",
    "tid"
);

1;
