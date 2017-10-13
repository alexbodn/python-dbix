use utf8;

package Icecat::Schema::Result::TidIndex;

=head1 NAME

Icecat::Schema::Result::TidIndex

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';

=head1 TABLE

tid_index

=cut

__PACKAGE__->table("tid_index");
__PACKAGE__->add_columns(
    tid   => { data_type => "integer",  is_auto_increment => 1 },
    dummy => { data_type => "smallint", is_nullable       => 1 },
);
__PACKAGE__->set_primary_key("tid");

__PACKAGE__->has_many(
    categories => "Icecat::Schema::Result::Category",
    "tid"
);

__PACKAGE__->has_many(
    features => "Icecat::Schema::Result::Feature",
    "tid"
);

__PACKAGE__->has_many(
    measures => "Icecat::Schema::Result::Measure",
    "tid"
);

__PACKAGE__->has_many(
    product_families => "Icecat::Schema::Result::ProductFamily",
    "tid"
);

__PACKAGE__->has_many(
    product_series => "Icecat::Schema::Result::ProductSeries",
    "tid"
);

__PACKAGE__->has_many(
    texts => "Icecat::Schema::Result::Tex",
    "tid"
);

1;
