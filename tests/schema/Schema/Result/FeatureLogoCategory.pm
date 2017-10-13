use utf8;

package Icecat::Schema::Result::FeatureLogoCategory;

=head1 NAME

Icecat::Schema::Result::FeatureLogoCategory

=head1 DESCRIPTION

Link table between L<Icecat::Schema::Result::FeatureLogo> and
L<Icecat::Schema::Result::Category>.

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';

=head1 TABLE

feature_logo_category

=cut

__PACKAGE__->table("feature_logo_category");

__PACKAGE__->add_columns(
    feature_logo_id => { data_type => "integer" },
    category_id     => { data_type => "integer" },
);
__PACKAGE__->set_primary_key( "feature_logo_id", "category_id" );

__PACKAGE__->belongs_to(
    feature_logo => "Icecat::Schema::Result::FeatureLogo",
    "feature_logo_id"
);

__PACKAGE__->belongs_to(
    category => "Icecat::Schema::Result::Category",
    { 'foreign.catid' => 'self.category_id' }
);

1;
