from django.shortcuts import render
from django.http import HttpResponseRedirect
# <HINT> Import any new Models here
from .models import Course, Enrollment, Question, Choice, Submission
from django.contrib.auth.models import User
from django.shortcuts import get_object_or_404, render, redirect
from django.urls import reverse
from django.views import generic
from django.contrib.auth import login, logout, authenticate
import logging
# Get an instance of a logger
logger = logging.getLogger(__name__)
logger.info("Houston, we have a %s", "interesting problem", exc_info=1)
# Create your views here.


def registration_request(request):
    context = {}
    if request.method == 'GET':
        return render(request, 'onlinecourse/user_registration_bootstrap.html', context)
    elif request.method == 'POST':
        # Check if user exists
        username = request.POST['username']
        password = request.POST['psw']
        first_name = request.POST['firstname']
        last_name = request.POST['lastname']
        user_exist = False
        try:
            User.objects.get(username=username)
            user_exist = True
        except:
            logger.error("New user")
        if not user_exist:
            user = User.objects.create_user(username=username, first_name=first_name, last_name=last_name,
                                            password=password)
            login(request, user)
            return redirect("onlinecourse:index")
        else:
            context['message'] = "User already exists."
            return render(request, 'onlinecourse/user_registration_bootstrap.html', context)


def login_request(request):
    context = {}
    if request.method == "POST":
        username = request.POST['username']
        password = request.POST['psw']
        user = authenticate(username=username, password=password)
        if user is not None:
            login(request, user)
            return redirect('onlinecourse:index')
        else:
            context['message'] = "Invalid username or password."
            return render(request, 'onlinecourse/user_login_bootstrap.html', context)
    else:
        return render(request, 'onlinecourse/user_login_bootstrap.html', context)


def logout_request(request):
    logout(request)
    return redirect('onlinecourse:index')


def check_if_enrolled(user, course):
    is_enrolled = False
    if user.id is not None:
        # Check if user enrolled
        num_results = Enrollment.objects.filter(user=user, course=course).count()
        if num_results > 0:
            is_enrolled = True
    return is_enrolled


# CourseListView
class CourseListView(generic.ListView):
    template_name = 'onlinecourse/course_list_bootstrap.html'
    context_object_name = 'course_list'

    def get_queryset(self):
        user = self.request.user
        courses = Course.objects.order_by('-total_enrollment')[:10]
        for course in courses:
            if user.is_authenticated:
                course.is_enrolled = check_if_enrolled(user, course)
        return courses


class CourseDetailView(generic.DetailView):
    model = Course
    template_name = 'onlinecourse/course_detail_bootstrap.html'


def enroll(request, course_id):
    course = get_object_or_404(Course, pk=course_id)
    user = request.user

    is_enrolled = check_if_enrolled(user, course)
    if not is_enrolled and user.is_authenticated:
        # Create an enrollment
        Enrollment.objects.create(user=user, course=course, mode='honor')
        course.total_enrollment += 1
        course.save()

    return HttpResponseRedirect(reverse(viewname='onlinecourse:course_details', args=(course.id,)))


# <HINT> Create a submit view to create an exam submission record for a course enrollment,
# you may implement it based on following logic:
         # Get user and course object, then get the associated enrollment object created when the user enrolled the course
         # Create a submission object referring to the enrollment
         # Collect the selected choices from exam form
         # Add each selected choice object to the submission object
         # Redirect to show_exam_result with the submission id
def submit(request, course_id):
    user = request.user
    course = get_object_or_404(Course, pk=course_id)
    enrollment = get_object_or_404(Enrollment.objects.filter(user=user, course=course))
    submission = Submission.objects.create(enrollment=enrollment)
    
    submitted_answers = [] # currently not in use
    for key in request.POST:
        if key.startswith('choice'):
            value = request.POST[key]
            choice_id = int(value)
            this_choice=Choice.objects.get(pk=choice_id)
            submitted_answers.append(choice_id) # currently not in use
            submission.choices.add(this_choice) #= choice_id # add each collected choice object to the submission object 
    #return submitted_answers
    return HttpResponseRedirect(reverse(viewname='onlinecourse:show_exam_result', args=(course.id, submission.id)))

# <HINT> A example method to collect the selected choices from the exam form from the request object
#def extract_answers(request):
#    submitted_anwsers = []
#    for key in request.POST:
#        if key.startswith('choice'):
#            value = request.POST[key]
#            choice_id = int(value)
#            submitted_anwsers.append(choice_id)
#    return submitted_anwsers


# <HINT> Create an exam result view to check if learner passed exam and show their question results and result for each question,
# you may implement it based on the following logic:
        # Get course and submission based on their ids
        # Get the selected choice ids from the submission record
        # For each selected choice, check if it is a correct answer or not
        # Calculate the total score

def merge_two_dicts(x, y):
    z = x.copy()   # start with keys and values of x
    z.update(y)    # modifies z with keys and values of y
    return z

def show_exam_result(request,course_id, submission_id):
    my_answers  = []
    my_questions = []
    question_choices= Choice.objects.none() #initialize empty choice-set...
    question_choices_all = Choice.objects.none() # initialize empty choice-set
    submission_choices= Choice.objects.none() #initialize empty choice-set...
    right_answers = []
    score = 0
    counter = 0
    grade = 0
    context = {}
    context['submission_id'] = submission_id
    context['course_id'] = course_id  
    context['message'] = "This is a test view for Exam results of course{}".format(course_id)
    
    course_questions = Question.objects.filter(course__id = course_id)
    
    context['questions'] = course_questions.values()

    for questions in course_questions:
        question_choices = question_choices.union(Choice.objects.filter(question__id = questions.id, correct = True))
        question_choices_all = question_choices_all.union(Choice.objects.filter(question__id = questions.id))
        submission_choices = submission_choices.union(Choice.objects.filter(question__id = questions.id, submission__id = submission_id))

        counter +=1
        if list(question_choices.values()) == list(submission_choices.values()): # create two dictionaries via values out of object queryset
            score +=1
    
    question_choices = question_choices.values()  # generate dictionary for context via values()
    answer_ids = submission_choices.values_list('id')
    submission_choices = submission_choices.values()
    
    grade = score/counter
   
    # submission_choices = Choice.objects.filter(submission__id = submission_id)
    # for my_choices in submission_choices:
    #     my_answers.append(my_choices.id)
    # 

    # #logger.info("Houston, we have a %s", "interesting problem", exc_info=1)
    # course_questions = Question.objects.filter(course__id = course_id)
    # for questions in course_questions:
    #     my_questions.append(questions.id)
    #     question_choices = Choice.objects.filter(question__id = questions.id)
    #     for right_choices in question_choices:
    #         if right_choices.correct == True:
    #             right_answers.append(right_choices.id)
    #     if right_answers in my_answers:
    #         score +=1

    context['score'] = score         
    context['solutions'] = question_choices
    context['answers'] = submission_choices
    context['all_choices'] = question_choices_all.values()
    context['grade'] = round((grade*100))
    context['my_answer_ids'] = answer_ids
   

   
    #return render(request, 'onlinecourse/user_registration_bootstrap.html', context)
    return render(request, 'onlinecourse/exam_result_bootstrap.html', context)



