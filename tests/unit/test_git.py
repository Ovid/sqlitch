"""
Tests for Git integration utilities.
"""

import os
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest

from sqlitch.utils.git import (
    GitRepository, GitStatus, VCSError,
    detect_vcs, get_vcs_user_info, is_vcs_clean, suggest_change_name
)


class TestGitRepository:
    """Test GitRepository class."""
    
    def test_init_with_path(self, tmp_path):
        """Test GitRepository initialization with path."""
        repo = GitRepository(tmp_path)
        assert repo.path == tmp_path
        assert repo._git_dir is None
        assert not repo.is_repository
    
    def test_init_without_path(self):
        """Test GitRepository initialization without path."""
        with patch('pathlib.Path.cwd') as mock_cwd:
            mock_cwd.return_value = Path('/test/path')
            repo = GitRepository()
            assert repo.path == Path('/test/path')
    
    def test_find_git_dir_exists(self, tmp_path):
        """Test finding .git directory when it exists."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        repo = GitRepository(tmp_path)
        assert repo._git_dir == git_dir
        assert repo.is_repository
        assert repo.root_path == tmp_path
    
    def test_find_git_dir_parent(self, tmp_path):
        """Test finding .git directory in parent directory."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        subdir = tmp_path / 'subdir'
        subdir.mkdir()
        
        repo = GitRepository(subdir)
        assert repo._git_dir == git_dir
        assert repo.is_repository
        assert repo.root_path == tmp_path
    
    def test_find_git_dir_not_found(self, tmp_path):
        """Test when .git directory is not found."""
        repo = GitRepository(tmp_path)
        assert repo._git_dir is None
        assert not repo.is_repository
        assert repo.root_path is None
    
    @patch('subprocess.run')
    def test_run_git_command_success(self, mock_run, tmp_path):
        """Test successful git command execution."""
        mock_run.return_value = Mock(returncode=0, stdout='output', stderr='')
        
        repo = GitRepository(tmp_path)
        result = repo._run_git_command(['status'])
        
        assert result.returncode == 0
        assert result.stdout == 'output'
        mock_run.assert_called_once_with(
            ['git', 'status'],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30
        )
    
    @patch('subprocess.run')
    def test_run_git_command_failure(self, mock_run, tmp_path):
        """Test git command failure."""
        mock_run.return_value = Mock(returncode=1, stdout='', stderr='error')
        
        repo = GitRepository(tmp_path)
        with pytest.raises(VCSError, match="Git command failed"):
            repo._run_git_command(['status'])
    
    @patch('subprocess.run')
    def test_run_git_command_no_check(self, mock_run, tmp_path):
        """Test git command with check=False."""
        mock_run.return_value = Mock(returncode=1, stdout='', stderr='error')
        
        repo = GitRepository(tmp_path)
        result = repo._run_git_command(['status'], check=False)
        
        assert result.returncode == 1
    
    @patch('subprocess.run')
    def test_run_git_command_timeout(self, mock_run, tmp_path):
        """Test git command timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired(['git'], 30)
        
        repo = GitRepository(tmp_path)
        with pytest.raises(VCSError, match="Git command timed out"):
            repo._run_git_command(['status'])
    
    @patch('subprocess.run')
    def test_run_git_command_not_found(self, mock_run, tmp_path):
        """Test git command not found."""
        mock_run.side_effect = FileNotFoundError()
        
        repo = GitRepository(tmp_path)
        with pytest.raises(VCSError, match="Git command not found"):
            repo._run_git_command(['status'])
    
    def test_get_status_not_repository(self, tmp_path):
        """Test get_status when not in a repository."""
        repo = GitRepository(tmp_path)
        status = repo.get_status()
        
        assert not status.is_repo
        assert status.is_clean
        assert status.current_branch is None
        assert status.current_commit is None
        assert not status.has_staged_changes
        assert not status.has_unstaged_changes
        assert status.untracked_files == []
    
    @patch('subprocess.run')
    def test_get_status_clean_repository(self, mock_run, tmp_path):
        """Test get_status for clean repository."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        # Mock git commands
        def side_effect(cmd, **kwargs):
            if 'rev-parse' in cmd and '--abbrev-ref' in cmd:
                return Mock(returncode=0, stdout='main\n')
            elif 'rev-parse' in cmd and 'HEAD' in cmd:
                return Mock(returncode=0, stdout='abc123\n')
            elif 'status' in cmd and '--porcelain' in cmd:
                return Mock(returncode=0, stdout='')
            return Mock(returncode=1, stdout='', stderr='')
        
        mock_run.side_effect = side_effect
        
        repo = GitRepository(tmp_path)
        status = repo.get_status()
        
        assert status.is_repo
        assert status.is_clean
        assert status.current_branch == 'main'
        assert status.current_commit == 'abc123'
        assert not status.has_staged_changes
        assert not status.has_unstaged_changes
        assert status.untracked_files == []
    
    @patch('subprocess.run')
    def test_get_status_dirty_repository(self, mock_run, tmp_path):
        """Test get_status for dirty repository."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        # Mock git commands
        def side_effect(cmd, **kwargs):
            if 'rev-parse' in cmd and '--abbrev-ref' in cmd:
                return Mock(returncode=0, stdout='feature\n')
            elif 'rev-parse' in cmd and 'HEAD' in cmd:
                return Mock(returncode=0, stdout='def456\n')
            elif 'status' in cmd and '--porcelain' in cmd:
                return Mock(returncode=0, stdout='M  modified.txt\n A  staged.txt\n?? untracked.txt\n')
            return Mock(returncode=1, stdout='', stderr='')
        
        mock_run.side_effect = side_effect
        
        repo = GitRepository(tmp_path)
        status = repo.get_status()
        
        assert status.is_repo
        assert not status.is_clean
        assert status.current_branch == 'feature'
        assert status.current_commit == 'def456'
        assert status.has_staged_changes
        assert status.has_unstaged_changes
        assert 'untracked.txt' in status.untracked_files
    
    @patch('subprocess.run')
    def test_get_status_git_error(self, mock_run, tmp_path):
        """Test get_status when git commands fail."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        mock_run.side_effect = subprocess.TimeoutExpired(['git'], 30)
        
        repo = GitRepository(tmp_path)
        status = repo.get_status()
        
        # Should return clean state on error
        assert status.is_repo
        assert status.is_clean
        assert status.current_branch is None
        assert status.current_commit is None
    
    @patch('subprocess.run')
    def test_get_user_name(self, mock_run, tmp_path):
        """Test getting Git user name."""
        mock_run.return_value = Mock(returncode=0, stdout='John Doe\n')
        
        repo = GitRepository(tmp_path)
        name = repo.get_user_name()
        
        assert name == 'John Doe'
        mock_run.assert_called_once_with(
            ['git', 'config', '--get', 'user.name'],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30
        )
    
    @patch('subprocess.run')
    def test_get_user_name_not_configured(self, mock_run, tmp_path):
        """Test getting Git user name when not configured."""
        mock_run.return_value = Mock(returncode=1, stdout='', stderr='')
        
        repo = GitRepository(tmp_path)
        name = repo.get_user_name()
        
        assert name is None
    
    @patch('subprocess.run')
    def test_get_user_name_error(self, mock_run, tmp_path):
        """Test getting Git user name with error."""
        mock_run.side_effect = subprocess.TimeoutExpired(['git'], 30)
        
        repo = GitRepository(tmp_path)
        name = repo.get_user_name()
        
        assert name is None
    
    @patch('subprocess.run')
    def test_get_user_email(self, mock_run, tmp_path):
        """Test getting Git user email."""
        mock_run.return_value = Mock(returncode=0, stdout='john@example.com\n')
        
        repo = GitRepository(tmp_path)
        email = repo.get_user_email()
        
        assert email == 'john@example.com'
        mock_run.assert_called_once_with(
            ['git', 'config', '--get', 'user.email'],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30
        )
    
    @patch('subprocess.run')
    def test_get_user_email_not_configured(self, mock_run, tmp_path):
        """Test getting Git user email when not configured."""
        mock_run.return_value = Mock(returncode=1, stdout='', stderr='')
        
        repo = GitRepository(tmp_path)
        email = repo.get_user_email()
        
        assert email is None
    
    @patch('subprocess.run')
    def test_init_repository(self, mock_run, tmp_path):
        """Test initializing Git repository."""
        mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
        
        repo = GitRepository(tmp_path)
        repo.init_repository()
        
        mock_run.assert_called_once_with(
            ['git', 'init'],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30
        )
        assert repo._git_dir == tmp_path / '.git'
    
    def test_init_repository_already_exists(self, tmp_path):
        """Test initializing Git repository when it already exists."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        repo = GitRepository(tmp_path)
        
        with patch.object(repo, '_run_git_command') as mock_run:
            repo.init_repository()
            mock_run.assert_not_called()
    
    @patch('subprocess.run')
    def test_add_files(self, mock_run, tmp_path):
        """Test adding files to Git."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
        
        repo = GitRepository(tmp_path)
        files = [Path('file1.txt'), Path('file2.txt')]
        repo.add_files(files)
        
        mock_run.assert_called_once_with(
            ['git', 'add', 'file1.txt', 'file2.txt'],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30
        )
    
    def test_add_files_not_repository(self, tmp_path):
        """Test adding files when not in repository."""
        repo = GitRepository(tmp_path)
        files = [Path('file1.txt')]
        
        with pytest.raises(VCSError, match="Not a Git repository"):
            repo.add_files(files)
    
    @patch('subprocess.run')
    def test_commit(self, mock_run, tmp_path):
        """Test committing changes."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        def side_effect(cmd, **kwargs):
            if 'commit' in cmd:
                return Mock(returncode=0, stdout='', stderr='')
            elif 'rev-parse' in cmd:
                return Mock(returncode=0, stdout='abc123\n')
            return Mock(returncode=1, stdout='', stderr='')
        
        mock_run.side_effect = side_effect
        
        repo = GitRepository(tmp_path)
        commit_hash = repo.commit('Test commit')
        
        assert commit_hash == 'abc123'
        assert mock_run.call_count == 2
    
    @patch('subprocess.run')
    def test_commit_with_author(self, mock_run, tmp_path):
        """Test committing changes with author."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        def side_effect(cmd, **kwargs):
            if 'commit' in cmd:
                return Mock(returncode=0, stdout='', stderr='')
            elif 'rev-parse' in cmd:
                return Mock(returncode=0, stdout='def456\n')
            return Mock(returncode=1, stdout='', stderr='')
        
        mock_run.side_effect = side_effect
        
        repo = GitRepository(tmp_path)
        commit_hash = repo.commit('Test commit', 'John Doe <john@example.com>')
        
        assert commit_hash == 'def456'
        # Check that --author was included in commit command
        commit_call = mock_run.call_args_list[0]
        assert '--author' in commit_call[0][0]
        assert 'John Doe <john@example.com>' in commit_call[0][0]
    
    def test_commit_not_repository(self, tmp_path):
        """Test committing when not in repository."""
        repo = GitRepository(tmp_path)
        
        with pytest.raises(VCSError, match="Not a Git repository"):
            repo.commit('Test commit')
    
    @patch('subprocess.run')
    def test_get_file_history(self, mock_run, tmp_path):
        """Test getting file history."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        mock_run.return_value = Mock(
            returncode=0,
            stdout='abc123 Initial commit\ndef456 Second commit\n'
        )
        
        repo = GitRepository(tmp_path)
        history = repo.get_file_history(Path('test.txt'), limit=5)
        
        assert len(history) == 2
        assert history[0]['hash'] == 'abc123'
        assert history[0]['message'] == 'Initial commit'
        assert history[1]['hash'] == 'def456'
        assert history[1]['message'] == 'Second commit'
        
        mock_run.assert_called_once_with(
            ['git', 'log', '--oneline', '-5', '--', 'test.txt'],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30
        )
    
    def test_get_file_history_not_repository(self, tmp_path):
        """Test getting file history when not in repository."""
        repo = GitRepository(tmp_path)
        history = repo.get_file_history(Path('test.txt'))
        
        assert history == []
    
    @patch('subprocess.run')
    def test_get_file_history_error(self, mock_run, tmp_path):
        """Test getting file history with error."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        mock_run.return_value = Mock(returncode=1, stdout='', stderr='error')
        
        repo = GitRepository(tmp_path)
        history = repo.get_file_history(Path('test.txt'))
        
        assert history == []
    
    @patch('subprocess.run')
    def test_is_file_tracked(self, mock_run, tmp_path):
        """Test checking if file is tracked."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        mock_run.return_value = Mock(returncode=0, stdout='', stderr='')
        
        repo = GitRepository(tmp_path)
        is_tracked = repo.is_file_tracked(Path('test.txt'))
        
        assert is_tracked
        mock_run.assert_called_once_with(
            ['git', 'ls-files', '--error-unmatch', 'test.txt'],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=30
        )
    
    @patch('subprocess.run')
    def test_is_file_not_tracked(self, mock_run, tmp_path):
        """Test checking if file is not tracked."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        mock_run.return_value = Mock(returncode=1, stdout='', stderr='')
        
        repo = GitRepository(tmp_path)
        is_tracked = repo.is_file_tracked(Path('test.txt'))
        
        assert not is_tracked
    
    def test_is_file_tracked_not_repository(self, tmp_path):
        """Test checking if file is tracked when not in repository."""
        repo = GitRepository(tmp_path)
        is_tracked = repo.is_file_tracked(Path('test.txt'))
        
        assert not is_tracked
    
    def test_get_relative_path(self, tmp_path):
        """Test getting relative path."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        repo = GitRepository(tmp_path)
        file_path = tmp_path / 'subdir' / 'file.txt'
        relative_path = repo.get_relative_path(file_path)
        
        assert relative_path == Path('subdir/file.txt')
    
    def test_get_relative_path_not_repository(self, tmp_path):
        """Test getting relative path when not in repository."""
        repo = GitRepository(tmp_path)
        file_path = tmp_path / 'file.txt'
        relative_path = repo.get_relative_path(file_path)
        
        assert relative_path is None
    
    def test_get_relative_path_outside_repo(self, tmp_path):
        """Test getting relative path for file outside repository."""
        git_dir = tmp_path / '.git'
        git_dir.mkdir()
        
        repo = GitRepository(tmp_path)
        file_path = Path('/other/path/file.txt')
        relative_path = repo.get_relative_path(file_path)
        
        assert relative_path is None


class TestGitStatus:
    """Test GitStatus dataclass."""
    
    def test_git_status_creation(self):
        """Test GitStatus creation."""
        status = GitStatus(
            is_repo=True,
            is_clean=False,
            current_branch='main',
            current_commit='abc123',
            has_staged_changes=True,
            has_unstaged_changes=False,
            untracked_files=['file.txt']
        )
        
        assert status.is_repo
        assert not status.is_clean
        assert status.current_branch == 'main'
        assert status.current_commit == 'abc123'
        assert status.has_staged_changes
        assert not status.has_unstaged_changes
        assert status.untracked_files == ['file.txt']


class TestUtilityFunctions:
    """Test utility functions."""
    
    @patch('sqlitch.utils.git.GitRepository')
    def test_detect_vcs_found(self, mock_git_repo, tmp_path):
        """Test VCS detection when repository found."""
        mock_repo = Mock()
        mock_repo.is_repository = True
        mock_git_repo.return_value = mock_repo
        
        vcs = detect_vcs(tmp_path)
        
        assert vcs == mock_repo
        mock_git_repo.assert_called_once_with(tmp_path)
    
    @patch('sqlitch.utils.git.GitRepository')
    def test_detect_vcs_not_found(self, mock_git_repo, tmp_path):
        """Test VCS detection when repository not found."""
        mock_repo = Mock()
        mock_repo.is_repository = False
        mock_git_repo.return_value = mock_repo
        
        vcs = detect_vcs(tmp_path)
        
        assert vcs is None
    
    def test_detect_vcs_no_path(self):
        """Test VCS detection without path."""
        with patch('sqlitch.utils.git.GitRepository') as mock_git_repo:
            mock_repo = Mock()
            mock_repo.is_repository = True
            mock_git_repo.return_value = mock_repo
            
            vcs = detect_vcs()
            
            assert vcs == mock_repo
            mock_git_repo.assert_called_once_with(None)
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_get_vcs_user_info_found(self, mock_detect_vcs, tmp_path):
        """Test getting VCS user info when VCS found."""
        mock_vcs = Mock()
        mock_vcs.get_user_name.return_value = 'John Doe'
        mock_vcs.get_user_email.return_value = 'john@example.com'
        mock_detect_vcs.return_value = mock_vcs
        
        name, email = get_vcs_user_info(tmp_path)
        
        assert name == 'John Doe'
        assert email == 'john@example.com'
        mock_detect_vcs.assert_called_once_with(tmp_path)
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_get_vcs_user_info_not_found(self, mock_detect_vcs, tmp_path):
        """Test getting VCS user info when VCS not found."""
        mock_detect_vcs.return_value = None
        
        name, email = get_vcs_user_info(tmp_path)
        
        assert name is None
        assert email is None
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_is_vcs_clean_found_clean(self, mock_detect_vcs, tmp_path):
        """Test VCS clean check when VCS found and clean."""
        mock_vcs = Mock()
        mock_status = Mock()
        mock_status.is_clean = True
        mock_vcs.get_status.return_value = mock_status
        mock_detect_vcs.return_value = mock_vcs
        
        is_clean = is_vcs_clean(tmp_path)
        
        assert is_clean
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_is_vcs_clean_found_dirty(self, mock_detect_vcs, tmp_path):
        """Test VCS clean check when VCS found and dirty."""
        mock_vcs = Mock()
        mock_status = Mock()
        mock_status.is_clean = False
        mock_vcs.get_status.return_value = mock_status
        mock_detect_vcs.return_value = mock_vcs
        
        is_clean = is_vcs_clean(tmp_path)
        
        assert not is_clean
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_is_vcs_clean_not_found(self, mock_detect_vcs, tmp_path):
        """Test VCS clean check when VCS not found."""
        mock_detect_vcs.return_value = None
        
        is_clean = is_vcs_clean(tmp_path)
        
        assert is_clean  # No VCS means "clean"
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_suggest_change_name_no_vcs(self, mock_detect_vcs, tmp_path):
        """Test change name suggestion when no VCS."""
        mock_detect_vcs.return_value = None
        
        name = suggest_change_name('my_change', tmp_path)
        
        assert name == 'my_change'
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_suggest_change_name_main_branch(self, mock_detect_vcs, tmp_path):
        """Test change name suggestion on main branch."""
        mock_vcs = Mock()
        mock_status = Mock()
        mock_status.current_branch = 'main'
        mock_vcs.get_status.return_value = mock_status
        mock_detect_vcs.return_value = mock_vcs
        
        name = suggest_change_name('my_change', tmp_path)
        
        assert name == 'my_change'
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_suggest_change_name_master_branch(self, mock_detect_vcs, tmp_path):
        """Test change name suggestion on master branch."""
        mock_vcs = Mock()
        mock_status = Mock()
        mock_status.current_branch = 'master'
        mock_vcs.get_status.return_value = mock_status
        mock_detect_vcs.return_value = mock_vcs
        
        name = suggest_change_name('my_change', tmp_path)
        
        assert name == 'my_change'
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_suggest_change_name_develop_branch(self, mock_detect_vcs, tmp_path):
        """Test change name suggestion on develop branch."""
        mock_vcs = Mock()
        mock_status = Mock()
        mock_status.current_branch = 'develop'
        mock_vcs.get_status.return_value = mock_status
        mock_detect_vcs.return_value = mock_vcs
        
        name = suggest_change_name('my_change', tmp_path)
        
        assert name == 'my_change'
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_suggest_change_name_feature_branch(self, mock_detect_vcs, tmp_path):
        """Test change name suggestion on feature branch."""
        mock_vcs = Mock()
        mock_status = Mock()
        mock_status.current_branch = 'feature/user-auth'
        mock_vcs.get_status.return_value = mock_status
        mock_detect_vcs.return_value = mock_vcs
        
        name = suggest_change_name('my_change', tmp_path)
        
        assert name == 'my_change_feature_user_auth'
    
    @patch('sqlitch.utils.git.detect_vcs')
    def test_suggest_change_name_bugfix_branch(self, mock_detect_vcs, tmp_path):
        """Test change name suggestion on bugfix branch."""
        mock_vcs = Mock()
        mock_status = Mock()
        mock_status.current_branch = 'bugfix/fix-login'
        mock_vcs.get_status.return_value = mock_status
        mock_detect_vcs.return_value = mock_vcs
        
        name = suggest_change_name('my_change', tmp_path)
        
        assert name == 'my_change_bugfix_fix_login'


class TestVCSError:
    """Test VCSError exception."""
    
    def test_vcs_error_creation(self):
        """Test VCSError creation."""
        error = VCSError("Test error")
        assert str(error) == "Test error"  # SqlitchError.__str__ returns just the message
        assert isinstance(error, Exception)