use utf8;

package Icecat::Schema::Result::ProductFeatureLocal;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );
__PACKAGE__->table("product_feature_local");
__PACKAGE__->add_columns(
    product_feature_local_id =>
      { data_type => "integer", is_auto_increment => 1 },
    product_id          => { data_type => "integer", default_value => 0 },
    category_feature_id => { data_type => "integer", default_value => 0 },
    value               => {
        data_type     => "varchar",
        default_value => "",
        size          => 15000,
    },
    langid  => { data_type => "integer", default_value => 0 },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
);
__PACKAGE__->set_primary_key("product_feature_local_id");
__PACKAGE__->add_unique_constraint(
    [ "category_feature_id", "product_id", "langid" ],
);

__PACKAGE__->belongs_to(
    category_feature => "Icecat::Schema::Result::CategoryFeature",
    "category_feature_id"
);

__PACKAGE__->belongs_to(
    language => "Icecat::Schema::Result::Language",
    "langid"
);

__PACKAGE__->belongs_to(
    product => "Icecat::Schema::Result::Product",
    "product_id"
);

1;
