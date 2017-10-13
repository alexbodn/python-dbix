use utf8;

package Icecat::Schema::Result::CategoryFeatureGroup;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->table("category_feature_group");
__PACKAGE__->add_columns(
    category_feature_group_id => { data_type => "integer" },
    catid                     => { data_type => "integer" },
    feature_group_id          => { data_type => "integer" },
    no                        => { data_type => "integer", is_nullable => 1 },
);
__PACKAGE__->set_primary_key("category_feature_group_id");
__PACKAGE__->add_unique_constraint( [ "catid", "feature_group_id" ] );

__PACKAGE__->belongs_to(
    category => "Icecat::Schema::Result::Category",
    "catid"
);

__PACKAGE__->has_many(
    category_features => "Icecat::Schema::Result::CategoryFeature",
    "category_feature_group_id"
);

__PACKAGE__->belongs_to(
    feature_group => "Icecat::Schema::Result::FeatureGroup",
    "feature_group_id"
);

1;
