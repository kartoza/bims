# coding=utf-8
import os
from collections import deque
from datetime import datetime

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin, UserPassesTestMixin
from django.core.files import File
from django.db import connection
from django.db.models import Q
from django.http import HttpResponseRedirect
from django.views.generic import TemplateView

from bims.models.harvest_session import HarvestSession, HarvestTrigger
from bims.models.taxon_group import TaxonGroup
from bims.tasks.harvest_worms_species import harvest_worms_species


class HarvestWormsSpeciesView(UserPassesTestMixin, LoginRequiredMixin, TemplateView):
    template_name = 'harvest_worms_species.html'

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
            category='worms',
        )

        if active_sessions.exists():
            session = active_sessions.last()
            session_data = {
                'module_group': session.module_group,
                'finished': session.finished,
                'start_time': str(session.start_time),
                'status': session.status,
                'id': session.id,
                'aphia_id': (session.additional_data or {}).get('aphia_id', ''),
            }
            try:
                with open(session.log_file.path, 'rb') as f:
                    session_data['log'] = b''.join(
                        list(deque(f, 50))).decode('utf-8')
            except (OSError, ValueError):
                session_data['log'] = ''
            ctx['upload_session'] = session_data

        ctx['finished_sessions'] = HarvestSession.objects.filter(
            Q(finished=True) | Q(canceled=True),
            harvester=self.request.user,
            category='worms',
        ).order_by('-start_time')

        return ctx

    def post(self, request, *args, **kwargs):
        # ---- cancellation ----
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

        # ---- start new harvest ----
        taxon_group_id = request.POST.get('taxon_group')
        aphia_id_raw = request.POST.get('aphia_id', '').strip()
        harvest_synonyms = request.POST.get('harvest_synonyms_for_accepted') == '1'

        if not taxon_group_id:
            messages.error(request, 'Please select a taxon group.')
            return HttpResponseRedirect(request.path_info)

        if not aphia_id_raw or not aphia_id_raw.isdigit():
            messages.error(request, 'Please enter a valid numeric WoRMS AphiaID.')
            return HttpResponseRedirect(request.path_info)

        aphia_id = int(aphia_id_raw)

        harvest_session = HarvestSession.objects.create(
            harvester=request.user,
            start_time=datetime.now(),
            module_group_id=taxon_group_id,
            category='worms',
            is_fetching_species=True,
            harvest_synonyms=harvest_synonyms,
            additional_data={'aphia_id': aphia_id},
        )

        log_folder = os.path.join(settings.MEDIA_ROOT, 'harvest-worms-session-log')
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

        harvest_worms_species.delay(
            harvest_session.id,
            schema_name=connection.schema_name,
        )
        return HttpResponseRedirect(request.path_info)
