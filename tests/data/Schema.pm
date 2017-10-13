use utf8;
package Icecat::Schema;

=head1 NAME

Icecat::Schema - DBIx::Class schema for icecat.biz data

=head1 VERSION

0.004

=cut

our $VERSION = 0.004;

use strict;
use warnings;

use base 'DBIx::Class::Schema::Config';

__PACKAGE__->load_namespaces;

1;

=head1 DESCRIPTION

DBIx::Class schema for L<Icecat.biz|http://icecat.biz/> product datasheets.

You can use L<DBIx::Class::Schema::Config> for database credential storage.

=head1 LOADING FROM XML

Run the following scripts:

=over

=item

    bin/load_xml

=item Retrieve XML files per category

    bin/get_products

This saves all products into F<xml/products> directory.

=item Load products into the database

    bin/load_products

This processes all products from the F<xml/products> directory.

=back

=head1 RESULT CLASSES

=over

=item * L<CategoryFeatureGroup|Icecat::Schema::Result::CategoryFeatureGroup>

=item * L<CategoryFeature|Icecat::Schema::Result::CategoryFeature>

=item * L<CategoryKeyword|Icecat::Schema::Result::CategoryKeyword>

=item * L<Category|Icecat::Schema::Result::Category>

=item * L<FeatureGroup|Icecat::Schema::Result::FeatureGroup>

=item * L<FeatureLogoCategory|Icecat::Schema::Result::FeatureLogoCategory>

=item * L<FeatureLogo|Icecat::Schema::Result::FeatureLogo>

=item * L<Feature|Icecat::Schema::Result::Feature>

=item * L<Language|Icecat::Schema::Result::Language>

=item * L<Measure|Icecat::Schema::Result::Measure>

=item * L<MeasureSign|Icecat::Schema::Result::MeasureSign>

=item * L<ProductDescription|Icecat::Schema::Result::ProductDescription>

=item * L<ProductFamily|Icecat::Schema::Result::ProductFamily>

=item * L<ProductFeatureLocal|Icecat::Schema::Result::ProductFeatureLocal>

=item * L<ProductFeature|Icecat::Schema::Result::ProductFeature>

=item * L<ProductGallery|Icecat::Schema::Result::ProductGallery>

=item * L<ProductMultimediaObject|Icecat::Schema::Result::ProductMultimediaObject>

=item * L<Product|Icecat::Schema::Result::Product>

=item * L<ProductRelated|Icecat::Schema::Result::ProductRelated>

=item * L<ProductReview|Icecat::Schema::Result::ProductReview>

=item * L<ProductSeries|Icecat::Schema::Result::ProductSeries>

=item * L<SidIndex|Icecat::Schema::Result::SidIndex>

Used for generating alternate keys for relationships with
L<Vocabulary|Icecat::Schema::Result::Vocabulary>.

=item * L<Supplier|Icecat::Schema::Result::Supplier>

=item * L<Tex|Icecat::Schema::Result::Tex>

Long I18N translations.

=item * L<TidIndex|Icecat::Schema::Result::TidIndex>

Used for generating alternate keys for relationships with
L<Tex|Icecat::Schema::Result::Tex>.

=item * L<Vocabulary|Icecat::Schema::Result::Vocabulary>

Short I18N translations.

=back

=cut
