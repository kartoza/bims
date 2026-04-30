# coding=utf-8
"""Upload view (GitHub App auth)"""
from typing import Optional, Tuple
from datetime import timedelta

import requests
from django.utils import timezone
from django.core.cache import cache

from django.http import JsonResponse, Http404
from django.urls import reverse_lazy
from django.views.generic import FormView
from django.contrib.auth.mixins import UserPassesTestMixin
from django.utils.translation import gettext as _

from preferences import preferences
from github import Github, Auth
from github.GithubException import GithubException, BadCredentialsException, UnknownObjectException
from github import GithubIntegration

from bims.forms.upload import UploadForm
from bims.models.upload_request import UploadRequest
from bims.models.licence import Licence


def get_client_ip(request) -> Optional[str]:
    xff = request.META.get('HTTP_X_FORWARDED_FOR')
    if xff:
        return xff.split(',')[0].strip()
    return request.META.get('REMOTE_ADDR')


def _normalize_pem(pem: str) -> str:
    if pem and "\\n" in pem and "BEGIN" not in pem:
        return pem.replace("\\n", "\n")
    return pem


def _get_installation_token_and_repo(repo_full_name: str) -> Tuple[str, str]:
    """
    Returns (installation_access_token, repo_full_name) using GitHub App auth.
    """
    app_id = preferences.SiteSetting.github_app_id
    private_key = _normalize_pem(preferences.SiteSetting.github_private_key)

    auth = Auth.AppAuth(app_id, private_key)

    if not app_id or not private_key:
        raise Http404("Missing GitHub App configuration (app id / private key)")

    integ = GithubIntegration(auth=auth)

    try:
        owner, name = repo_full_name.split("/", 1)
    except ValueError:
        raise Http404("Invalid repo name, expected 'owner/repo'")
    try:
        installation = integ.get_repo_installation(owner, name)
        installation_id = installation.id
    except UnknownObjectException:
        raise Http404("App is not installed on the target repository")

    cache_key = f"ghapp_install_token:{installation_id}"
    cached = cache.get(cache_key)
    if cached and cached.get("exp") and cached["exp"] > timezone.now() + timedelta(minutes=5):
        return cached["token"], repo_full_name

    access = integ.get_access_token(installation_id)
    token = access.token

    expires_at = access.expires_at if hasattr(
        access, "expires_at") else timezone.now() + timedelta(minutes=55)
    cache.set(cache_key,
              {"token": token, "exp": expires_at}, timeout=55 * 60)

    return token, repo_full_name


class UploadView(UserPassesTestMixin, FormView):
    """Upload view."""
    form_class = UploadForm
    template_name = 'uploader.html'
    success_url = reverse_lazy('upload')
    context_data = dict()

    def _recaptcha_enabled(self) -> bool:
        return bool(preferences.SiteSetting.recaptcha_site_key and preferences.SiteSetting.recaptcha_secret_key)

    def _get_recaptcha_token(self) -> str:
        return (
                self.request.POST.get('recaptcha_token')
                or self.request.POST.get('g-recaptcha-response')
                or ''
        ).strip()

    def _verify_recaptcha(self, token: str) -> tuple[bool, str]:
        """
        Verify with Google.
        """
        if not token:
            return False, "missing-token"

        try:
            resp = requests.post(
                'https://www.google.com/recaptcha/api/siteverify',
                data={
                    'secret': preferences.SiteSetting.recaptcha_secret_key,
                    'response': token,
                    'remoteip': get_client_ip(self.request) or '',
                },
                timeout=8,
            )
        except requests.RequestException:
            return False, "network-error"

        try:
            payload = resp.json()
        except ValueError:
            return False, "invalid-json"

        if not payload.get('success'):
            return False, ",".join(payload.get('error-codes', [])) or "verification-failed"

        score = payload.get('score')
        action = payload.get('action')
        min_score = 0.5
        expected_action = 'upload'

        if score is not None and score < float(min_score):
            return False, f"low-score:{score}"

        if expected_action and action and action != expected_action:
            return False, f"wrong-action:{action}"

        return True, "ok"

    def test_func(self):
        return True

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        ctx['licences'] = Licence.objects.all()
        return ctx

    def get_initial(self):
        initial = super().get_initial()
        u = getattr(self.request, "user", None)
        if u and u.is_authenticated:
            full_name = ((f"{u.first_name} {u.last_name}").strip()
                         or getattr(u, "get_full_name", lambda: "")().strip()
                         or u.email or u.get_username())
            initial.update({"name": full_name, "email": u.email})
        return initial

    def form_invalid(self, form):
        return JsonResponse({'status': 'ERROR', 'errors': form.errors}, status=400)

    def _get_repo_via_app(self):
        repo_name = getattr(preferences.SiteSetting, 'github_feedback_repo', '')
        if not repo_name:
            raise Http404('Missing GitHub repository configuration')
        token, repo_full = _get_installation_token_and_repo(repo_name)
        gh = Github(auth=Auth.Token(token))
        return gh.get_repo(repo_full, lazy=False)

    def _create_github_issue(self, instance: UploadRequest):
        """
        Create a GitHub ticket using the GitHub App installation token.
        """
        repo = self._get_repo_via_app()

        labels = [
            'upload',
            preferences.SiteSetting.default_site_name
        ]

        assignees = preferences.SiteSetting.github_upload_assignees.split(',')

        file_url = self.request.build_absolute_uri(instance.upload_file.url)
        licence_display = instance.licence.name if instance.licence else '-'
        gbif_consent_display = 'Yes' if instance.gbif_consent else 'No'
        body = (
            f"**New Upload**\n\n"
            f"- **Name:** {instance.name}\n"
            f"- **Email:** {instance.email}\n"
            f"- **Type:** {instance.upload_type.name}\n"
            f"- **Data Licence:** {licence_display}\n"
            f"- **GBIF Publishing Consent:** {gbif_consent_display}\n"
            f"- **Notes:** {instance.notes or '-'}\n"
            f"- **File:** [{instance.file_name}]({file_url})\n"
            f"- **Upload ID:** {instance.pk}\n"
        )

        title = f"{instance.title}"

        issue = repo.create_issue(
            title=title,
            body=body,
            labels=labels,
            assignees=assignees
        )
        return issue

    def form_valid(self, form):
        if self._recaptcha_enabled():
            token = self._get_recaptcha_token()
            ok, reason = self._verify_recaptcha(token)
            if not ok:
                return JsonResponse(
                    {
                        'status': 'ERROR',
                        'message': _('reCAPTCHA verification failed. Please try again.'),
                        'reason': reason,
                        'errors': {'recaptcha': [_('Please complete the reCAPTCHA.')]}
                    },
                    status=400
                )

        instance = UploadRequest(
            title=form.cleaned_data['title'],
            user=self.request.user if self.request.user.is_authenticated else None,
            name=form.cleaned_data['name'],
            email=form.cleaned_data['email'],
            upload_type=form.cleaned_data['upload_type'],
            licence=form.cleaned_data.get('licence'),
            notes=form.cleaned_data.get('notes') or '',
            gbif_consent=form.cleaned_data.get('gbif_consent') or False,
            source=form.cleaned_data.get('source') or 'upload_portal',
            client_ip=get_client_ip(self.request),
            status=UploadRequest.STATUS_SUBMITTED,
        )
        instance.upload_file = form.cleaned_data['upload_file']
        instance.save()

        try:
            issue = self._create_github_issue(instance)
            instance.github_issue_number = issue.number
            instance.github_issue_url = getattr(issue, 'html_url', '') \
                or f"https://github.com/{issue.repository.full_name}/issues/{issue.number}"
            instance.status = UploadRequest.STATUS_TICKETED
            instance.save(update_fields=['github_issue_number', 'github_issue_url', 'status'])
        except BadCredentialsException:
            instance.status = UploadRequest.STATUS_ERROR
            instance.save(update_fields=['status'])
            raise Http404('Invalid GitHub App credentials')
        except UnknownObjectException:
            instance.status = UploadRequest.STATUS_ERROR
            instance.save(update_fields=['status'])
            raise Http404('GitHub repository or installation not found')
        except GithubException as ge:
            instance.status = UploadRequest.STATUS_ERROR
            instance.save(update_fields=['status'])
            msg = ge.data.get('message') if getattr(ge, 'data', None) else str(ge)
            raise Http404(f'GitHub error: {msg}')

        return JsonResponse({
            'status': 'OK',
            'message': _('Thank you — your upload was received.'),
            'upload_id': instance.pk,
            'ticket_number': instance.github_issue_number,
            'issue_url': instance.github_issue_url,
            'file_url': self.request.build_absolute_uri(instance.upload_file.url),
        })
