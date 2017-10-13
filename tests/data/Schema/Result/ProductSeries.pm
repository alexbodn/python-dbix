use utf8;

package Icecat::Schema::Result::ProductSeries;

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->table("product_series");
__PACKAGE__->add_columns(
    "series_id"   => { data_type => "integer" },
    "sid"         => { data_type => "integer" },
    "tid"         => { data_type => "integer" },
    "supplier_id" => { data_type => "integer" },
    "catid"       => { data_type => "integer", is_nullable => 1 },
    "family_id"   => { data_type => "integer" },
);
__PACKAGE__->set_primary_key("series_id");

__PACKAGE__->belongs_to(
    category => "Icecat::Schema::Result::Category",
    "catid"
);

__PACKAGE__->has_many(
    descriptions => "Icecat::Schema::Result::Tex",
    { 'foreign.tid' => 'self.tid' }
);

__PACKAGE__->belongs_to(
    product_family => "Icecat::Schema::Result::ProductFamily",
    "family_id"
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
    supplier => "Icecat::Schema::Result::Supplier",
    "supplier_id"
);

__PACKAGE__->belongs_to(
    tidindex => "Icecat::Schema::Result::TidIndex",
    "tid"
);

1;
