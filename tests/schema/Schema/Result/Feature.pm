use utf8;

package Icecat::Schema::Result::Feature;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );
__PACKAGE__->table("feature");
__PACKAGE__->add_columns(
    feature_id        => { data_type => "integer" },
    sid               => { data_type => "integer" },
    tid               => { data_type => "integer" },
    measure_id        => { data_type => "integer", is_nullable => 1 },
    type              => { data_type => "varchar", size => 60 },
    class             => { data_type => "tinyint", default_value => 0 },
    limit_direction   => { data_type => "tinyint", default_value => 0 },
    searchable        => { data_type => "tinyint", default_value => 0 },
    restricted_values => { data_type => "mediumtext", is_nullable => 1 },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
    last_published => {
        data_type     => "integer",
        default_value => 0,
        is_nullable   => 1
    },
);
__PACKAGE__->set_primary_key("feature_id");

__PACKAGE__->has_many(
    category_features => "Icecat::Schema::Result::CategoryFeature",
    "feature_id"
);

__PACKAGE__->has_many(
    descriptions => "Icecat::Schema::Result::Tex",
    { 'foreign.tid' => 'self.tid' }
);

__PACKAGE__->has_many(
    feature_logos => "Icecat::Schema::Result::FeatureLogo",
    "feature_id"
);

__PACKAGE__->belongs_to(
    measure => "Icecat::Schema::Result::Measure",
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

__PACKAGE__->belongs_to(
    tidindex => "Icecat::Schema::Result::TidIndex",
    "tid"
);

1;
