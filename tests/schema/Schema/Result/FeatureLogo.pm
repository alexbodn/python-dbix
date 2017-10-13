use utf8;

package Icecat::Schema::Result::FeatureLogo;

=head1 NAME

Icecat::Schema::Result::FeatureLogo

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';
__PACKAGE__->load_components( "InflateColumn::DateTime", "TimeStamp" );

=head1 TABLE

feature_logo

=cut

__PACKAGE__->table("feature_logo");

__PACKAGE__->add_columns(
    feature_logo_id         => { data_type => "integer", is_auto_increment => 1 },
    feature_id => { data_type => "integer" },
    values     => { data_type => "varchar", default_value => "", size => 255 },
    link       => { data_type => "varchar", size => 255 },
    height     => { data_type => "integer" },
    width      => { data_type => "integer" },
    size       => { data_type => "integer" },
    thumb_link => { data_type => "varchar", default_value => "", size => 255 },
    thumb_size => { data_type => "integer", default_value => 0 },
);

__PACKAGE__->set_primary_key("feature_logo_id");

__PACKAGE__->add_unique_constraint( ['link'] );

__PACKAGE__->has_many(
    feature_logo_categories => "Icecat::Schema::Result::FeatureLogoCategory",
    "feature_logo_id"
);

__PACKAGE__->belongs_to(
    feature => "Icecat::Schema::Result::Feature",
    "feature_id"
);

1;
