use utf8;

package Icecat::Schema::Result::ProductFamily;

=head1 NAME

Icecat::Schema::Result::ProductFamily

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );

=head1 TABLE

product_family

=cut

__PACKAGE__->table("product_family");
__PACKAGE__->add_columns(
    family_id        => { data_type => "integer" },
    parent_family_id => { data_type => "integer", is_nullable => 1 },
    supplier_id      => { data_type => "integer" },
    sid              => { data_type => "integer" },
    tid              => { data_type => "integer" },
    low_pic => { data_type => "varchar", is_nullable => 1, size => 255 },
    thumb_pic => { data_type => "varchar", is_nullable => 1, size => 255 },
    catid     => { data_type => "integer", is_nullable => 1 },
    data_source_id => { data_type => "integer", default_value => 0 },
    symbol => { data_type => "varchar", default_value => "", size => 120 },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
);
__PACKAGE__->set_primary_key("family_id");

__PACKAGE__->belongs_to(
    category => "Icecat::Schema::Result::Category",
    "catid"
);

__PACKAGE__->has_many(
    children => "Icecat::Schema::Result::ProductFamily",
    { 'foreign.parent_family_id' => 'self.family_id'  }
);

__PACKAGE__->has_many(
    descriptions => "Icecat::Schema::Result::Tex",
    { 'foreign.tid' => 'self.tid' }
);

__PACKAGE__->has_many(
    names => "Icecat::Schema::Result::Vocabulary",
    { 'foreign.sid' => 'self.sid' }
);

__PACKAGE__->belongs_to(
    parent => "Icecat::Schema::Result::ProductFamily",
    { 'foreign.family_id' => 'self.parent_family_id' }, { join_type => "left" }
);

__PACKAGE__->has_many(
    product_series => "Icecat::Schema::Result::ProductSeries",
    "family_id"
);

__PACKAGE__->belongs_to(
    sidindex => "Icecat::Schema::Result::SidIndex",
    "sid"
);

__PACKAGE__->belongs_to(
    supplier => "Icecat::Schema::Result::Supplier",
    "supplier_id"
);

__PACKAGE__->belongs_to(
    tidindex => "Icecat::Schema::Result::TidIndex",
    "tid"
);

1;
