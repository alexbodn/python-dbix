use utf8;

package Icecat::Schema::Result::ProductRelated;

=head1 NAME

Icecat::Schema::Result::ProductRelated

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );

=head1 TABLE

product_related

=cut

__PACKAGE__->table("product_related");
__PACKAGE__->add_columns(
    product_related_id => { data_type => "integer", is_auto_increment => 1 },
    product_id         => { data_type => "integer" },
    rel_product_id     => { data_type => "integer" },
    updated            => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
    preferred_option => { data_type => "tinyint",  default_value => 0 },
    data_source_id   => { data_type => "integer",  default_value => 0 },
    order            => { data_type => "smallint", default_value => 0, },
);
__PACKAGE__->set_primary_key("product_related_id");
__PACKAGE__->add_unique_constraint( [ "product_id", "rel_product_id" ] );

__PACKAGE__->belongs_to(
    product => "Icecat::Schema::Result::Product",
    "product_id"
);

__PACKAGE__->belongs_to(
    related_product => "Icecat::Schema::Result::Product",
    { 'foreign.product_id' => 'self.rel_product_id' }
);

1;
