export const taxonDetail = (() => {

    function parseAcceptedTaxonomyName(raw) {
      const out = { canonical: '', genus: '', species: '', subspecies: '', author: '', addendum: '' };
      if (!raw || !String(raw).trim()) return out;

      let s = String(raw).trim().replace(/\s*-\s*[A-Z][A-Z\s.]+$/u, '').trim();
      // Take out the addendum (s.l. / sensu lato / partim) shown after the name
      const addendumPattern = /\s+(s\.\s?l\.|sensu\s+lato|partim)(?=\s|$)/iu;
      const addendumMatch = s.match(addendumPattern);
      if (addendumMatch) {
        out.addendum = /^partim$/iu.test(addendumMatch[1]) ? 'partim' : 'sensu lato';
        s = s.replace(addendumPattern, '').trim();
      }

      const paren = s.match(/\(([^()]*)\)\s*$/u);
      if (paren) {
        out.author = paren[1].trim();
        s = s.replace(/\s*\([^)]*\)\s*$/u, '').trim();
      }

      const tokens = s.split(/\s+/);
      if (!tokens.length) return out;

      out.genus = tokens[0] || '';
      let i = 1;

      if (tokens[i] && /^[a-z]/u.test(tokens[i])) { out.species = tokens[i]; i += 1; }
      if (tokens[i] && /^[a-z]/u.test(tokens[i])) { out.subspecies = tokens[i]; i += 1; }

      if (!out.author && i < tokens.length) {
        const trailing = tokens.slice(i).join(' ').trim();
        if (/(^|[\s,])\d{3,4}\s*$/.test(trailing)) {
          out.author = trailing;
        }
      }

      out.canonical = [out.genus, out.species, out.subspecies].filter(Boolean).join(' ');
      return out;
    }

    function getCleanAcceptedName(data, key = 'SPECIES') {
      const name = data?.accepted_taxonomy_name ?? '';
      const parts = parseAcceptedTaxonomyName(name);

      // Prefer explicit key; otherwise fall back to data.rank; default SPECIES.
      const k = String(key || data?.rank || 'SPECIES').toUpperCase();

      switch (k) {
        case 'GENUS':       return parts.genus;
        case 'SPECIES':     return parts.species;
        case 'SUBSPECIES':  return parts.subspecies;
        case 'AUTHOR':      return parts.author;
        case 'ADDENDUM':    return parts.addendum;
        case 'CANONICAL':   return parts.canonical;
        default:            return parts.canonical;
      }
    }

    function normalizeStatus(status) {
        return String(status || '').trim().toUpperCase();
    }

    function shouldUseAcceptedHierarchy(data) {
        const status = normalizeStatus(data?.taxonomic_status);
        return Boolean(status.includes('SYNONYM') && data?.accepted_taxonomy);
    }

    function getHierarchySource(data) {
        if (shouldUseAcceptedHierarchy(data) && data?._accepted_detail) {
            return data._accepted_detail;
        }
        return data;
    }

    function escapeHtml(value) {
        return String(value ?? '').replace(/[&<>"']/g, ch => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[ch]));
    }

    function sameRankName(a, b) {
        return String(a || '').trim().toLowerCase() === String(b || '').trim().toLowerCase();
    }

    // A synonym can have been described under a different genus, tribe or
    // family than its accepted taxon. The upload keeps the original
    // classification in additional_data; show the levels that differ from the
    // accepted hierarchy (all of them when that hierarchy is not available).
    function originalClassification(data, hierarchySource) {
        const extra = data?.additional_data;
        if (!extra) {
            return '';
        }
        const compareWith = hierarchySource && hierarchySource !== data ? hierarchySource : null;
        const parts = [
            ['Family', 'family', 'Family'],
            ['SubFamily', 'subfamily', 'Subfamily'],
            ['Tribe', 'tribe', 'Tribe'],
            ['SubTribe', 'subtribe', 'Subtribe'],
            ['Genus', 'genus', 'Genus'],
        ]
            .filter(([key, acceptedKey]) => extra[key] &&
                !(compareWith && sameRankName(extra[key], compareWith[acceptedKey])))
            .map(([key, , label]) => `${label}: <strong>${escapeHtml(extra[key])}</strong>`);
        if (!parts.length) {
            return '';
        }
        return `<br/>As originally classified - ${parts.join(' &middot; ')}`;
    }

    function acceptedHierarchyNotice(data, hierarchySource) {
        if (!shouldUseAcceptedHierarchy(data)) {
            return '';
        }
        const originalNote = originalClassification(data, hierarchySource);
        if (!hierarchySource || hierarchySource === data) {
            const statusLabel = (data?.taxonomic_status || 'Synonym').replace(/_/g, ' ');
            return `
                <div class="alert alert-warning synonym-summary mb-3 mx-4">
                    <strong>${statusLabel}</strong>: Accepted hierarchy data is unavailable.${originalNote}
                </div>
            `;
        }
        const statusLabel = (data?.taxonomic_status || 'Synonym').replace(/_/g, ' ');
        const acceptedName = hierarchySource.canonical_name
            || getCleanAcceptedName(data, 'CANONICAL')
            || 'Accepted taxon';
        const safeAcceptedName = acceptedName.replace(/"/g, '&quot;');
        return `
            <div class="alert alert-info synonym-summary mb-3 mx-4">
                <strong>${statusLabel}</strong>: Showing hierarchical information for
                <a href="#" class="accepted-taxon-link" data-accepted-name="${safeAcceptedName}"><strong>${acceptedName}</strong></a>. Metadata below refers to
                <strong>${data.canonical_name}</strong>.${originalNote}
            </div>
        `;
    }

    function formatDetailTaxon(data, options = {}) {
        const _taxonworksBaseUrl = ((options.taxonworksBaseUrl || '')).replace(/\/+$/, '');
        const hierarchySource = getHierarchySource(data);
        const notice = acceptedHierarchyNotice(data, hierarchySource);
        // This section describes the valid/accepted name, so a synonym shows
        // its accepted taxon's addendum rather than its own.
        const acceptedAddendum = data.accepted_taxonomy_name
            ? getCleanAcceptedName(data, 'ADDENDUM')
            : data.addendum;

        return `
            <div class="container container-fluid" style="padding-left:40px;">
                <div class="row">
                    ${notice}
                </div>
                <div class="row mb-2">
                    <div class="dt-column">
                        <div class="row">
                            <div class="col-12 text-white dt-header">
                                Supra-generic Information
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Kingdom</strong></div> 
                                <div class="col-6">${hierarchySource.kingdom}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Phylum</strong></div>
                                <div class="col-6">${hierarchySource.phylum}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Class</strong></div>
                                <div class="col-6">${hierarchySource.class_name}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Order</strong></div>
                                <div class="col-6">${hierarchySource.order}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Family</strong></div>
                                <div class="col-6">${hierarchySource.family}</div>
                            </div>
                        </div>
                    </div>
                    <div class="dt-column">
                        <div class="row">
                            <div class="col-12 text-white dt-header">
                                Infra Categories
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Subfamily</strong></div>
                                <div class="col-6">${hierarchySource.subfamily}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Tribe</strong></div>
                                <div class="col-6">${hierarchySource.tribe}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Subtribe</strong></div>
                                <div class="col-6">${hierarchySource.subtribe}</div>
                            </div>
                        </div>
                    </div>
                    <div class="dt-column">
                        <div class="row">
                            <div class="col-12 text-white dt-header">
                                Valid/Accepted Names
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Genus</strong></div>
                                 <div class="col-6">${
                                    data.accepted_taxonomy_name
                                        ? getCleanAcceptedName(data, 'GENUS')
                                        : hierarchySource.genus
                                }</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Subgenus</strong></div>
                                <div class="col-6">${data.rank == 'SUBGENUS' && data.accepted_taxonomy_name ? data.accepted_taxonomy_name : hierarchySource.subgenus}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Species group</strong></div>
                                <div class="col-6">${hierarchySource.species_group}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Species</strong></div>
                                <div class="col-6">${
                                    data.accepted_taxonomy_name
                                        ? getCleanAcceptedName(data, 'SPECIES')
                                        : hierarchySource.species
                                }</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Subspecies</strong></div>
                                <div class="col-6">${
                                    data.accepted_taxonomy_name
                                        ? getCleanAcceptedName(data, 'SUBSPECIES')
                                        : hierarchySource.subspecies
                                }</div>
                            </div>
                            ${acceptedAddendum ? `
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Addendum</strong></div>
                                <div class="col-6">${acceptedAddendum}</div>
                            </div>
                            ` : ''}
                            <div class="dt-item col-12 row">
                                <div class="col-6"><strong>Author(s)</strong></div>
                                <div class="col-6">${
                                    data.accepted_taxonomy_name
                                        ? getCleanAcceptedName(data, 'AUTHOR')
                                        : hierarchySource.author
                                }</div>
                            </div>
                        </div>
                    </div>
                </div>           
            </div>
            <div class="container container-fluid" style="padding-left:40px;">
                <div class="row">
                    <div class="dt-full-column">
                        <div class="row">
                            <div class="col-12 text-white dt-header">
                                Additional Information
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Taxonomic Comments</strong></div>
                                <div class="col-8">${data.additional_data !== null ? (data.additional_data['Taxonomic Comments'] || data.additional_data['Taxonomic comments'] || '') : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Taxonomic References</strong></div>
                                <div class="col-8">${data.additional_data !== null ? (data.additional_data['Taxonomic References'] || data.additional_data['Species Name References'] || '') : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Biogeographic Comments</strong></div>
                                <div class="col-8">${data.additional_data !== null ? (data.additional_data['Biogeographic Comments'] || '') : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Biogeographic References</strong></div>
                                <div class="col-8">${data.additional_data !== null ? (data.additional_data['Biogeographic References'] || '') : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Environmental Comments</strong></div>
                                <div class="col-8">${data.additional_data !== null ? (data.additional_data['Environmental Comments'] || '') : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Environmental References</strong></div>
                                <div class="col-8">${data.additional_data !== null ? (data.additional_data['Environmental References'] || '') : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Conservation Status</strong></div>
                                <div class="col-8">${data.iucn_status_full_name}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Conservation Comments</strong></div>
                                <div class="col-8">${data.additional_data !== null ? (data.additional_data['Conservation Comments'] || '') : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Conservation References</strong></div>
                                <div class="col-8">${data.additional_data !== null ? (data.additional_data['Conservation References'] || '') : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>Common Name</strong></div>
                                <div class="col-8">${data.common_name !== 'Unknown' ? data.common_name : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>GBIF Key</strong></div>
                                <div class="col-8">${data.gbif_key ? data.gbif_key : ''}</div>
                            </div>
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>COL XR</strong></div>
                                <div class="col-8">${data.col_id ? data.col_id : ''}</div>
                            </div>
                            ${typeof isFadaSite !== 'undefined' && isFadaSite ? `
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>FADA ID</strong></div>
                                <div class="col-8">${data.fada_id ? data.fada_id : ''}</div>
                            </div>
                            ` : ''}
                            ${typeof data.aphia_id !== 'undefined' && data.aphia_id ? `
                                <div class="dt-item col-12 row">
                                    <div class="col-4"><strong>WoRMS AphiaID</strong></div>
                                    <div class="col-8"><a href="https://www.marinespecies.org/aphia.php?p=taxdetails&id=${data.aphia_id}" target="_blank" rel="noopener noreferrer">${data.aphia_id}</a></div>
                                </div>
                            `: ''}
                            ${data.taxonworks_otu_id && _taxonworksBaseUrl ? `
                                <div class="dt-item col-12 row">
                                    <div class="col-4"><strong>TaxonWorks OTU</strong></div>
                                    <div class="col-8"><a href="${_taxonworksBaseUrl}/${data.taxonworks_otu_id}" target="_blank" rel="noopener noreferrer">${data.taxonworks_otu_id}</a></div>
                                </div>
                            `: ''}
                            ${data.urls && data.urls.length ? `
                            <div class="dt-item col-12 row">
                                <div class="col-4"><strong>URL References</strong></div>
                                <div class="col-8">
                                    ${data.urls.map(u => `<div><a href="${u.uri}" target="_blank" rel="noopener noreferrer">${u.label}</a></div>`).join('')}
                                </div>
                            </div>
                            ` : ''}
                        </div>
                    </div>
                </div>
            </div>
`;
    }

    function getCanonicalName(name) {
        return parseAcceptedTaxonomyName(name).canonical;
    }

    return {
        formatDetailTaxon,
        shouldUseAcceptedHierarchy,
        getCanonicalName,
    };
})();
