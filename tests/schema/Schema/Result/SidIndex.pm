use utf8;

package Icecat::Schema::Result::SidIndex;

=head1 NAME

Icecat::Schema::Result::SidIndex

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';

=head1 TABLE

sid_index

=cut

__PACKAGE__->table("sid_index");

__PACKAGE__->add_columns(
    sid   => { data_type => "integer",  is_auto_increment => 1 },
    dummy => { data_type => "smallint", is_nullable       => 1 },
);

__PACKAGE__->set_primary_key("sid");

__PACKAGE__->has_many(
    categories => "Icecat::Schema::Result::Category",
    "sid"
);

__PACKAGE__->has_many(
    features => "Icecat::Schema::Result::Feature",
    "sid"
);

__PACKAGE__->has_many(
    feature_groups => "Icecat::Schema::Result::FeatureGroup",
    "sid"
);

__PACKAGE__->has_many(
    languages => "Icecat::Schema::Result::Language",
    "sid"
);

__PACKAGE__->has_many(
    measures => "Icecat::Schema::Result::Measure",
    "sid"
);

__PACKAGE__->has_many(
    product_families => "Icecat::Schema::Result::ProductFamily",
    "sid"
);

__PACKAGE__->has_many(
    product_series => "Icecat::Schema::Result::ProductSeries",
    "sid"
);

__PACKAGE__->has_many(
    vocabularies => "Icecat::Schema::Result::Vocabulary",
    "sid"
);

1;
