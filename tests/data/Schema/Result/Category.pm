use utf8;

package Icecat::Schema::Result::Category;

=head1 NAME

Icecat::Schema::Result::Category

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';

=head1 COMPONENTS

=over

=item * DBIx::Class::InflateColumn::DateTime

=item * DBIx::Class::TimeStamp

=item * DBIx::Class::Tree::AdjacencyList

=back

=cut

__PACKAGE__->load_components( "Tree::AdjacencyList", "InflateColumn::DateTime",
    "TimeStamp" );

=head1 TABLE

category

=cut

__PACKAGE__->table("category");

=head1 ACCESSORS

=head2 catid

Primary key.

=head2 ucatid

=head2 pcatid

Parent category ID. This is defined for all categories except for the top-level
catch-all category which is the parent of all other categories.

=head2 sid

Alternate key for L</names> relation.

=head2 tid

Alternate key for L</descriptions> relation.

=head2 searchable

=head2 low_pic

=head2 thumb_pic

=head2 updated

=head2 last_published

=head2 watched_top10

=head2 visible

=head2 ssid

=cut

__PACKAGE__->add_columns(
    catid  => { data_type => "integer" },
    ucatid => { data_type => "varchar", is_nullable => 1, size => 255 },
    pcatid     => { data_type => "integer", is_nullable   => 1 },
    sid        => { data_type => "integer" },
    tid        => { data_type => "integer" },
    searchable => { data_type => "integer", default_value => 0 },
    low_pic    => {
        data_type     => "varchar",
        default_value => "",
        size          => 255
    },
    thumb_pic => {
        data_type     => "varchar",
        default_value => "",
        is_nullable   => 1,
        size          => 255
    },
    updated => {
        data_type     => "datetime",
        set_on_create => 1,
        set_on_update => 1,
    },
    last_published =>
      { data_type => "integer", default_value => 0, is_nullable => 1 },
    watched_top10 => { data_type => "integer", default_value => 0 },
    visible       => { data_type => "integer", default_value => 0 },
    ssid          => { data_type => "integer", is_nullable => 1 },
);

__PACKAGE__->set_primary_key("catid");

__PACKAGE__->add_unique_constraint( "ucatid", ["ucatid"] );

__PACKAGE__->has_many(
    category_features => "Icecat::Schema::Result::CategoryFeature",
    "catid"
);

__PACKAGE__->has_many(
    category_feature_groups => "Icecat::Schema::Result::CategoryFeatureGroup",
    "catid"
);

__PACKAGE__->has_many(
    descriptions => "Icecat::Schema::Result::Tex",
    { 'foreign.tid' => 'self.tid' }
);

__PACKAGE__->has_many(
    feature_logo_categories => "Icecat::Schema::Result::FeatureLogoCategory",
    "category_id"
);

__PACKAGE__->has_many(
    keywords => "Icecat::Schema::Result::CategoryKeyword",
    "category_id"
);

__PACKAGE__->has_many(
    names => "Icecat::Schema::Result::Vocabulary",
    { 'foreign.sid' => 'self.sid' }
);

__PACKAGE__->has_many(
    products => "Icecat::Schema::Result::Product",
    "catid"
);

__PACKAGE__->has_many(
    product_families => "Icecat::Schema::Result::ProductFamily",
    "catid"
);

__PACKAGE__->has_many(
    product_series => "Icecat::Schema::Result::ProductSeries",
    "catid"
);

__PACKAGE__->belongs_to(
    sidindex => "Icecat::Schema::Result::SidIndex",
    "sid"
);

__PACKAGE__->belongs_to(
    tidindex => "Icecat::Schema::Result::TidIndex",
    "tid"
);

=head1 INHERITED METHODS

=head2 DBIx::Class::Tree::AdjacencyList

=over 4

=item * L<parent|DBIx::Class::Tree::AdjacencyList/parent>

=item * L<ancestors|DBIx::Class::Tree::AdjacencyList/ancestors>

=item * L<has_descendant|DBIx::Class::Tree::AdjacencyList/has_descendant>

=item * L<parents|DBIx::Class::Tree::AdjacencyList/parents>

=item * L<children|DBIx::Class::Tree::AdjacencyList/children>

=item * L<attach_child|DBIx::Class::Tree::AdjacencyList/attach_child>

=item * L<siblings|DBIx::Class::Tree::AdjacencyList/siblings>

=item * L<attach_sibling|DBIx::Class::Tree::AdjacencyList/attach_sibling>

=item * L<is_leaf|DBIx::Class::Tree::AdjacencyList/is_leaf>

=item * L<is_root|DBIx::Class::Tree::AdjacencyList/is_root>

=item * L<is_branch|DBIx::Class::Tree::AdjacencyList/is_branch>

=back

=cut

# define parent column

__PACKAGE__->parent_column('pcatid');

1;
