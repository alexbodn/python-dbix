use utf8;

package Icecat::Schema::Result::CategoryFeature;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );
__PACKAGE__->table("category_feature");
__PACKAGE__->add_columns(
    category_feature_id       => { data_type => "integer" },
    feature_id                => { data_type => "integer" },
    catid                     => { data_type => "integer" },
    no                        => { data_type => "integer", default_value => 0 },
    searchable                => { data_type => "integer", default_value => 0 },
    category_feature_group_id => { data_type => "integer", default_value => 0 },
    restricted_search_values => { data_type => "mediumtext", is_nullable => 1 },
    use_dropdown_input => {
        data_type     => "char",
        default_value => "",
        is_nullable   => 1,
        size          => 3,
    },
    mandatory => {
        data_type     => "tinyint",
        default_value => 0,
        is_nullable   => 1
    },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
);
__PACKAGE__->set_primary_key("category_feature_id");
__PACKAGE__->add_unique_constraint( [ "feature_id", "catid" ] );

__PACKAGE__->belongs_to(
    category => "Icecat::Schema::Result::Category",
    "catid"
);

__PACKAGE__->belongs_to(
    category_feature_group => "Icecat::Schema::Result::CategoryFeatureGroup",
    "category_feature_group_id"
);

__PACKAGE__->belongs_to(
    feature => "Icecat::Schema::Result::Feature",
    "feature_id"
);

__PACKAGE__->has_many(
    product_features => "Icecat::Schema::Result::ProductFeature",
    "category_feature_id"
);

__PACKAGE__->has_many(
    product_feature_locals => "Icecat::Schema::Result::ProductFeatureLocal",
    "category_feature_id"
);

1;
