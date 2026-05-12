# coding=utf-8
import os
from collections import deque
from datetime import datetime, timezone as dt_timezone

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.core.files import File
from django.db import connection
from django.db.models import Q
from django.http import HttpResponseRedirect
from django.views.generic import TemplateView

from bims.models.harvest_session import HarvestSession, HarvestTrigger


def _format_duration(session) -> str:
    """Return a human-readable duration string for a finished/canceled session."""
    data = session.additional_data or {}
    finished_at_str = data.get('finished_at')
    start = session.start_time
    if not start:
        return ''
    try:
        if not finished_at_str:
            return ''
        from datetime import datetime as _dt
        finished_at = _dt.fromisoformat(finished_at_str)
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=dt_timezone.utc)
        if start.tzinfo is None:
            start = start.replace(tzinfo=dt_timezone.utc)
        delta = finished_at - start
        total_seconds = int(delta.total_seconds())
        if total_seconds < 0:
            return ''
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f'{hours}h {minutes}m {seconds}s'
        if minutes:
            return f'{minutes}m {seconds}s'
        return f'{seconds}s'
    except Exception:
        return ''
from bims.models.taxon_group import TaxonGroup
from bims.tasks.harvest_taxonworks_species import harvest_taxonworks_species


class HarvestTaxonWorksSpeciesView(UserPassesTestMixin, LoginRequiredMixin, TemplateView):
    template_name = 'harvest_taxonworks_species.html'

    def test_func(self):
        return self.request.user.has_perm('bims.can_harvest_species')

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx['taxa_groups'] = TaxonGroup.objects.filter(
            category='SPECIES_MODULE',
        ).order_by('display_order')

        active_sessions = HarvestSession.objects.filter(
            Q(harvester=self.request.user) | Q(trigger=HarvestTrigger.SCHEDULED),
            finished=False,
            canceled=False,
            log_file__isnull=False,
            category='taxonworks',
        )

        if active_sessions.exists():
            session = active_sessions.last()
            session_data = {
                'module_group': session.module_group,
                'finished': session.finished,
                'start_time': str(session.start_time),
                'status': session.status,
                'id': session.id,
                'base_url': (session.additional_data or {}).get('base_url', ''),
                'taxon_name_id': (session.additional_data or {}).get('taxon_name_id', ''),
            }
            try:
                with open(session.log_file.path, 'rb') as f:
                    session_data['log'] = b''.join(
                        list(deque(f, 50))).decode('utf-8')
            except (OSError, ValueError):
                session_data['log'] = ''
            ctx['upload_session'] = session_data

        finished_sessions = HarvestSession.objects.filter(
            Q(finished=True) | Q(canceled=True),
            harvester=self.request.user,
            category='taxonworks',
        ).order_by('-start_time')

        for session in finished_sessions:
            session.duration_display = _format_duration(session)

        ctx['finished_sessions'] = finished_sessions

        seen = set()
        previous_configs = []
        for s in finished_sessions:
            data = s.additional_data or {}
            key = (
                data.get('base_url', ''),
                data.get('project_token', ''),
                str(data.get('taxon_name_id', '')),
                str(s.module_group_id or ''),
            )
            if key in seen or not data.get('base_url'):
                continue
            seen.add(key)
            previous_configs.append({
                'id': s.id,
                'label': '{group} - {base_url} / ID {taxon_id}'.format(
                    group=s.module_group.name if s.module_group else '—',
                    base_url=data.get('base_url', ''),
                    taxon_id=data.get('taxon_name_id', ''),
                ),
                'taxon_group_id': s.module_group_id,
                'base_url': data.get('base_url', ''),
                'project_token': data.get('project_token', ''),
                'taxon_name_id': data.get('taxon_name_id', ''),
                'exclude_extinct': data.get('exclude_extinct', True),
            })
        ctx['previous_configs'] = previous_configs
        return ctx

    def post(self, request, *args, **kwargs):
        if request.POST.get('cancel', 'False').lower() == 'true':
            try:
                session = HarvestSession.objects.get(
                    id=int(request.POST.get('canceled_session_id', '')),
                    harvester=request.user,
                )
                session.canceled = True
                session.save()
            except (HarvestSession.DoesNotExist, ValueError):
                pass
            return HttpResponseRedirect(request.path_info)

        taxon_group_id = request.POST.get('taxon_group')
        base_url = (request.POST.get('base_url') or '').strip()
        project_token = (request.POST.get('project_token') or '').strip()
        taxon_name_id_raw = (request.POST.get('taxon_name_id') or '').strip()
        exclude_extinct = request.POST.get('exclude_extinct') == '1'
        harvest_synonyms = request.POST.get('harvest_synonyms_for_accepted') == '1'

        if not taxon_group_id:
            messages.error(request, 'Please select a taxon group.')
            return HttpResponseRedirect(request.path_info)
        if not base_url:
            messages.error(request, 'Please enter a TaxonWorks base URL.')
            return HttpResponseRedirect(request.path_info)
        if not project_token:
            messages.error(request, 'Please enter a TaxonWorks project token.')
            return HttpResponseRedirect(request.path_info)
        if not taxon_name_id_raw or not taxon_name_id_raw.isdigit():
            messages.error(request, 'Please enter a valid numeric TaxonWorks taxon name ID.')
            return HttpResponseRedirect(request.path_info)

        harvest_session = HarvestSession.objects.create(
            harvester=request.user,
            start_time=datetime.now(),
            module_group_id=taxon_group_id,
            category='taxonworks',
            is_fetching_species=True,
            harvest_synonyms=harvest_synonyms,
            additional_data={
                'base_url': base_url,
                'project_token': project_token,
                'taxon_name_id': int(taxon_name_id_raw),
                'exclude_extinct': exclude_extinct,
            },
        )

        log_folder = os.path.join(settings.MEDIA_ROOT, 'harvest-taxonworks-session-log')
        if not os.path.exists(log_folder):
            os.mkdir(log_folder)

        log_path = os.path.join(
            log_folder,
            '{id}-{time}.txt'.format(
                id=harvest_session.id,
                time=harvest_session.start_time.strftime('%s'),
            )
        )

        with open(log_path, 'a+') as fi:
            harvest_session.log_file = File(fi, name=os.path.basename(fi.name))
            harvest_session.save()

        harvest_taxonworks_species.delay(
            harvest_session.id,
            schema_name=connection.schema_name,
        )
        return HttpResponseRedirect(request.path_info)
