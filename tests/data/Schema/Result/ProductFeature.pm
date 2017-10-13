use utf8;

package Icecat::Schema::Result::ProductFeature;

=head1 NAME

Icecat::Schema::Result::ProductFeature

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );

=head1 TABLE

product_feature

=cut

__PACKAGE__->table("product_feature");
__PACKAGE__->add_columns(
    product_feature_id => { data_type => "integer", is_auto_increment => 1 },
    product_id         => { data_type => "integer" },
    category_feature_id => { data_type => "integer" },
    value => {
        data_type     => "varchar",
        default_value => "",
        size          => 20000,
    },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
);
__PACKAGE__->set_primary_key("product_feature_id");
__PACKAGE__->add_unique_constraint( [ "category_feature_id", "product_id" ], );

__PACKAGE__->belongs_to(
    category_feature => "Icecat::Schema::Result::CategoryFeature",
    "category_feature_id"
);

__PACKAGE__->belongs_to(
    product => "Icecat::Schema::Result::Product",
    "product_id"
);

1;
